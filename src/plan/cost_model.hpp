#pragma once

namespace wing {

class CostCalculator {
 public:
  static double HashJoinCost(double build_size, double probe_size) { return 0; }

  /* Calculate the cost of nestloop join. */
  static double NestloopJoinCost(double build_size, double probe_size) {
    return 0;
  }

  /* Calculate the cost of sequential scan. */
  static double SeqScanCost(double size) { return 0; }
};

}  // namespace wing
