#include "topic_alive_monitor.hpp"

TopicAliveMonitor::TopicAliveMonitor() : Node("topic_alive_monitor") {
  // Read parameters from configuration or use defaults
  this->declare_parameter("topic_name", kDefaultTopic);
  this->declare_parameter("timeout", kDefaultTimeout);

  target_topic_ = this->get_parameter("topic_name").as_string();
  timeout_seconds_ = this->get_parameter("timeout").as_double();

  // Initialize the time tracking
  last_message_time_ = this->now();

  // Setup the subscriber (patata_sub_)
  target_topic_sub_ = this->create_subscription<std_msgs::msg::String>(
    target_topic_, 
    10,
    std::bind(&TopicAliveMonitor::message_callback, this, std::placeholders::_1));

  // Setup the watchdog timer
  auto timer_period = std::chrono::duration<double>(timeout_seconds_);
  watchdog_timer_ = this->create_wall_timer(
    timer_period, 
    std::bind(&TopicAliveMonitor::watchdog_callback, this));

  RCLCPP_INFO(this->get_logger(), "Monitor started. Topic: %s", target_topic_.c_str());
}

void TopicAliveMonitor::message_callback(const std_msgs::msg::String::SharedPtr msg) {
  // Mark the time whenever a new message is received
  (void)msg;  // Prevent unused variable warning
  last_message_time_ = this->now();
}

void TopicAliveMonitor::watchdog_callback() {
  // Calculate elapsed time using double (not float)
  double elapsed_seconds = (this->now() - last_message_time_).seconds();

  if (elapsed_seconds > timeout_seconds_) {
    RCLCPP_ERROR(
      this->get_logger(), 
      "FAILURE: No messages received on %s for %f seconds", 
      target_topic_.c_str(), 
      elapsed_seconds);
      
    // Print to stdout so the Python tester script can read the failure
    std::cout << "[ERROR] TIMEOUT DETECTED" << std::endl;
  }
}

int main(int argc, char **argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<TopicAliveMonitor>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
