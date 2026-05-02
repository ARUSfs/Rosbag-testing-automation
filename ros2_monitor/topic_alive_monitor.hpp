#ifndef CHECKERS_TOPIC_ALIVE_MONITOR_HPP_
#define CHECKERS_TOPIC_ALIVE_MONITOR_HPP_

#include <string>
#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>  // Change to your specific ROS2 message type

class TopicAliveMonitor : public rclcpp::Node {
public:
  TopicAliveMonitor();

private:
  // Node logic methods
  void message_callback(const std_msgs::msg::String::SharedPtr msg);
  void watchdog_callback();

  // Constants (kCamelCase, no trailing underscore)
  const double kDefaultTimeout = 1.0;
  const std::string kDefaultTopic = "/perception/map2";

  // Private variables (snake_case with trailing underscore)
  double timeout_seconds_;
  std::string target_topic_;
  rclcpp::Time last_message_time_;

  // ROS2 internal components (Subscribers with _sub_, Publishers with _pub_)
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr target_topic_sub_;
  rclcpp::TimerBase::SharedPtr watchdog_timer_;
};

#endif  // CHECKERS_TOPIC_ALIVE_MONITOR_HPP_
