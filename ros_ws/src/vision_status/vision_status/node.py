import time
from functools import partial

import rclpy
from rclpy.node import Node
from rclpy.qos import qos_profile_sensor_data, QoSProfile, ReliabilityPolicy, DurabilityPolicy
from sensor_msgs.msg import Image, CameraInfo, PointCloud2
from nav_msgs.msg import Odometry

from .monitor import StreamMonitor
from rclpy.time import Time
from rclpy.duration import Duration
from tf2_ros import Buffer, TransformListener, TransformException


class VisionStatus(Node):
    def __init__(self):
        super().__init__('vision_status')
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(
            self.tf_buffer, self
        )

        self.declare_parameter('tf_target_frame', 'odom')
        self.declare_parameter(
            'tf_source_frame',
            'zed_front_camera_link'
        )
        self.declare_parameter('stamp_max_age_sec', 0.5)
        defaults = {
            'rgb': ('/zed/zed_node/rgb/image_rect_color', Image),
            'depth': ('/zed/zed_node/depth/depth_registered', Image),
            'camera_info': ('/zed/zed_node/rgb/camera_info', CameraInfo),
            'odometry': ('/zed/zed_node/odom', Odometry),
            'cloud_map': ('/rtabmap/cloud_map', PointCloud2),
        }
        self.declare_parameter('timeout_sec', 3.0)
        self.declare_parameter('cloud_timeout_sec', 15.0)
        self.declare_parameter('report_interval_sec', 2.0)
        self.declare_parameter('rate_window_sec', 10.0)
        self.declare_parameter('startup_grace_sec', 10.0)
        self.start_time = time.monotonic()
        self.monitors = {}
        self.last_states = {}
        self.subscriptions_ = []
        timeout = float(self.get_parameter('timeout_sec').value)
        cloud_timeout = float(self.get_parameter('cloud_timeout_sec').value)
        window = float(self.get_parameter('rate_window_sec').value)
        self.grace = float(self.get_parameter('startup_grace_sec').value)
        if min(timeout, cloud_timeout, window, self.grace) < 0 or timeout == 0 or cloud_timeout == 0 or window == 0:
            raise ValueError('Timeouts and window must be positive; grace must be nonnegative')
        for name, (default_topic, msg_type) in defaults.items():
            self.declare_parameter(name + '_topic', default_topic)
            topic = self.get_parameter(name + '_topic').value
            self.monitors[name] = StreamMonitor(cloud_timeout if name == 'cloud_map' else timeout, window)
            self.last_states[name] = 'WAITING'
            # Sensor-data QoS works with typical best-effort ZED streams.
            qos = qos_profile_sensor_data
            self.subscriptions_.append(self.create_subscription(msg_type, topic, partial(self.on_message, name), qos))
            self.get_logger().info(f'Monitoring {name}: {topic}')
        interval = float(self.get_parameter('report_interval_sec').value)
        if interval <= 0:
            raise ValueError('report_interval_sec must be positive')
        self.timer = self.create_timer(interval, self.report)

    def on_message(self, name, msg):
        monitor = self.monitors[name]
        # Track message arrival
        monitor.record(time.monotonic())

        # Track timestamp inside message
        stamp = msg.header.stamp
        # Convert time to seconds
        stamp_sec = stamp.sec + stamp.nanosec * 1e-9
        monitor.record_stamp(stamp_sec)

    def check_tf(self):
        target = self.get_parameter('tf_target_frame').value
        source = self.get_parameter('tf_source_frame').value

        rgb_stamp = self.monitors['rgb'].last_stamp

        if rgb_stamp is None:
            return 'TF WAITING: no RGB timestamp'

        stamp_ns = int(round(rgb_stamp * 1_000_000_000))
        requested_time = Time(nanoseconds=stamp_ns)

        try:
            self.tf_buffer.lookup_transform(
                target,
                source,
                requested_time,
                timeout=Duration(seconds=0.0)
            )
            return f'TF OK: {target} -> {source}'

        except TransformException as exc:
            return f'TF WARN: {target} -> {source}: {exc}'
    
    def report(self):
        now = time.monotonic()
        lines = ['Vision pipeline status:']
        for name, monitor in self.monitors.items():
            state = monitor.status(now)
            age = monitor.age(now)
            lines.append(f'  {name:12} {state:8} {monitor.rate(now):6.2f} Hz  age={"never" if age is None else f"{age:.1f}s"}')
            if monitor.backward_count > 0:
                lines.append(
                    f'    TIMESTAMP WARN: {monitor.backward_count} backward jumps detected'
                )
            previous = self.last_states[name]
            if state == 'NO DATA' and previous != 'NO DATA':
                self.get_logger().warning(f'{name}: data timed out')
            elif state == 'OK' and previous == 'NO DATA':
                self.get_logger().info(f'{name}: data recovered')
            elif state == 'OK' and previous == 'WAITING':
                self.get_logger().info(f'{name}: first data received')
            elif state == 'WAITING' and now - self.start_time >= self.grace and previous == 'WAITING':
                # Warn once, but preserve WAITING until first message.
                self.get_logger().warning(f'{name}: still waiting for first message')
                self.last_states[name] = 'WAITING_WARNED'
            if state != 'WAITING' or previous != 'WAITING_WARNED':
                self.last_states[name] = state
        # Check how old each message timestamp is
        ros_now = self.get_clock().now().nanoseconds / 1e9

        max_age = float(
            self.get_parameter('stamp_max_age_sec').value
        )

        for name in ['rgb', 'depth', 'odometry']:
            monitor = self.monitors[name]

            if monitor.last_stamp is None:
                lines.append(f'{name.upper()} STAMP WAITING')
                continue

            if monitor.status(now) != 'OK':
                lines.append(f'{name.upper()} STAMP NO DATA')
                continue

            stamp_age = ros_now - monitor.last_stamp

            if stamp_age < -max_age:
                lines.append(
                    f'{name.upper()} STAMP WARN: '
                    f'{abs(stamp_age):.3f}s in the future'
                )
            elif stamp_age > max_age:
                lines.append(
                    f'{name.upper()} STAMP WARN: '
                    f'{stamp_age:.3f}s old'
                )
            else:
                lines.append(
                    f'{name.upper()} STAMP OK: '
                    f'age={stamp_age:.3f}s'
                )
        
        # Check timestamp synchronization between RGB, depth, and odometry
        sync_names = ['rgb', 'depth', 'odometry']

        stamps = {
            name: self.monitors[name].last_stamp
            for name in sync_names
        }

        if all(stamps[name] is not None and self.monitors[name].status(now) == 'OK'
            for name in sync_names
        ):
            newest = max(stamps.values())
            oldest = min(stamps.values())
            skew = newest - oldest

            if skew > 0.1:
                lines.append(
                    f'SYNC WARN: timestamp skew = {skew:.3f}s'
                )
            else:
                lines.append(
                    f'SYNC OK: timestamp skew = {skew:.3f}s'
                )
        else:
            lines.append('SYNC WAITING: missing timestamps')
        lines.append(self.check_tf())
        self.get_logger().info('\n'.join(lines))


def main(args=None):
    rclpy.init(args=args)
    node = None
    try:
        node = VisionStatus()
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if node is not None:
            node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
