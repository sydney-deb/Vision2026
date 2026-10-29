import time
from dataclasses import dataclass
from typing import Optional

import rclpy
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data
from rclpy.time import Time

from sensor_msgs.msg import Image, CameraInfo, PointCloud2
from nav_msgs.msg import Odometry
from tf2_ros import Buffer, TransformListener, TransformException


@dataclass
class StreamState:
    last_arrival: Optional[float] = None
    last_stamp: Optional[int] = None
    rate: Optional[float] = None
    backward_count: int = 0
    backward: bool = False


class VisionHealthMonitor(Node):
    def __init__(self):
        super().__init__("vision_health_monitor")

        defaults = {
            "rgb_topic": "/zed/zed_node/rgb/image_rect_color",
            "depth_topic": "/zed/zed_node/depth/depth_registered",
            "camera_info_topic": "/zed/zed_node/rgb/camera_info",
            "odom_topic": "/zed/zed_node/odom",
            "cloud_topic": "/rtabmap/cloud_map",
            "target_frame": "odom",
            "source_frame": "zed_camera_link",
            "arrival_timeout_sec": 3.0,
            "startup_timeout_sec": 10.0,
            "max_stamp_age_sec": 1.0,
            "future_tolerance_sec": 0.1,
            "sync_tolerance_sec": 0.1,
            "report_period_sec": 2.0,
        }

        for name, value in defaults.items():
            self.declare_parameter(name, value)

        self.streams = {}
        self.subscriptions_ = []
        self.started = time.monotonic()

        specifications = [
            ("rgb", Image, "rgb_topic"),
            ("depth", Image, "depth_topic"),
            ("camera_info", CameraInfo, "camera_info_topic"),
            ("odom", Odometry, "odom_topic"),
            ("cloud", PointCloud2, "cloud_topic"),
        ]

        for name, message_type, parameter in specifications:
            self.streams[name] = StreamState()
            topic = self.param(parameter)

            subscription = self.create_subscription(
                message_type,
                topic,
                lambda msg, stream=name: self.receive(stream, msg),
                qos_profile_sensor_data,
            )
            self.subscriptions_.append(subscription)

        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        self.timer = self.create_timer(
            self.param("report_period_sec"), self.report
        )

        self.get_logger().info(
            "Vision health monitor started. "
            "Timestamp checks require a shared ROS clock."
        )

    def param(self, name):
        return self.get_parameter(name).value

    def receive(self, name, message):
        state = self.streams[name]
        arrival = time.monotonic()

        if state.last_arrival is not None:
            interval = arrival - state.last_arrival
            if interval > 0:
                instantaneous_rate = 1.0 / interval
                if state.rate is None:
                    state.rate = instantaneous_rate
                else:
                    state.rate = (
                        0.2 * instantaneous_rate + 0.8 * state.rate
                    )

        state.last_arrival = arrival

        stamp = (
            message.header.stamp.sec * 1_000_000_000
            + message.header.stamp.nanosec
        )

        # Zero stamps are invalid for these timing checks.
        # Keep them visible, but do not count them as backward jumps.
        state.backward = (
            stamp > 0
            and state.last_stamp is not None
            and state.last_stamp > 0
            and stamp < state.last_stamp
        )

        if state.backward:
            state.backward_count += 1
            self.get_logger().warning(
                f"{name}: timestamp moved backward "
                f"(total={state.backward_count})"
            )

        state.last_stamp = stamp

    def stream_status(self, state, wall_now, ros_now_ns):
        if state.last_arrival is None:
            if wall_now - self.started > self.param(
                "startup_timeout_sec"
            ):
                return "NO DATA (never received)"
            return "WAITING"

        silence = wall_now - state.last_arrival

        if silence > self.param("arrival_timeout_sec"):
            return f"NO DATA ({silence:.1f}s since arrival)"

        if state.last_stamp is None or state.last_stamp == 0:
            return "ZERO/INVALID STAMP"

        age = (ros_now_ns - state.last_stamp) / 1_000_000_000
        problems = []

        if age > self.param("max_stamp_age_sec"):
            problems.append(f"STALE STAMP age={age:.3f}s")

        if age < -self.param("future_tolerance_sec"):
            problems.append(f"FUTURE STAMP ahead={-age:.3f}s")

        if state.backward:
            problems.append("BACKWARD STAMP")

        if problems:
            return "; ".join(problems)

        return f"OK age={age:.3f}s"

    def sync_status(self, wall_now, ros_now_ns):
        names = ("rgb", "depth", "odom")
        stamps = []

        for name in names:
            state = self.streams[name]

            if state.last_arrival is None:
                return f"WAITING for {name}"

            if (
                wall_now - state.last_arrival
                > self.param("arrival_timeout_sec")
            ):
                return f"UNAVAILABLE: {name} stopped arriving"

            if not state.last_stamp:
                return f"UNAVAILABLE: {name} has invalid stamp"

            age = (
                ros_now_ns - state.last_stamp
            ) / 1_000_000_000

            if (
                age > self.param("max_stamp_age_sec")
                or age < -self.param("future_tolerance_sec")
                or state.backward
            ):
                return f"UNAVAILABLE: {name} timestamp unhealthy"

            stamps.append(state.last_stamp)

        spread = (max(stamps) - min(stamps)) / 1_000_000_000
        status = (
            "OK"
            if spread <= self.param("sync_tolerance_sec")
            else "SKEW"
        )

        return f"{status}: latest-stamp spread={spread:.3f}s"

    def tf_status(self, wall_now, ros_now_ns):
        rgb = self.streams["rgb"]

        if rgb.last_arrival is None:
            return "WAITING for RGB timestamp"

        if (
            wall_now - rgb.last_arrival
            > self.param("arrival_timeout_sec")
        ):
            return "UNAVAILABLE: RGB stopped arriving"

        if not rgb.last_stamp:
            return "UNAVAILABLE: invalid RGB timestamp"

        age = (ros_now_ns - rgb.last_stamp) / 1_000_000_000

        if (
            age > self.param("max_stamp_age_sec")
            or age < -self.param("future_tolerance_sec")
            or rgb.backward
        ):
            return "UNAVAILABLE: RGB timestamp unhealthy"

        target = self.param("target_frame")
        source = self.param("source_frame")

        query_time = Time(
            nanoseconds=rgb.last_stamp,
            clock_type=self.get_clock().clock_type,
        )

        try:
            # Nonblocking lookup: callbacks can continue receiving TF.
            self.tf_buffer.lookup_transform(
                target,
                source,
                query_time,
                timeout=Duration(seconds=0.0),
            )
            return f"OK: {source} -> {target} at RGB timestamp"

        except TransformException as error:
            detail = " ".join(str(error).split())
            return (
                f"UNAVAILABLE: {source} -> {target}: "
                f"{type(error).__name__}: {detail}"
            )

    def report(self):
        wall_now = time.monotonic()
        ros_now_ns = self.get_clock().now().nanoseconds

        lines = ["VISION HEALTH"]

        for name, state in self.streams.items():
            status = self.stream_status(
                state, wall_now, ros_now_ns
            )
            rate = (
                "n/a"
                if state.rate is None
                else f"{state.rate:.1f} Hz"
            )

            lines.append(
                f"{name}: {status} | arrival rate={rate} "
                f"| backward jumps={state.backward_count}"
            )

        lines.append(
            "SYNC: " + self.sync_status(wall_now, ros_now_ns)
        )
        lines.append(
            "TF: " + self.tf_status(wall_now, ros_now_ns)
        )

        self.get_logger().info("\n" + "\n".join(lines))


def main(args=None):
    rclpy.init(args=args)
    node = VisionHealthMonitor()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()