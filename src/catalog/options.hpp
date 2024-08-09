#pragma once

#include <memory>

#include "execution/execoptions.hpp"
#include "plan/optimizeroptions.hpp"
#include "storage/lsm/options.hpp"

namespace wing {

class WingOptions {
 public:
  /* The size of batch in vectorized execution engine. */
  size_t size_batch{1024};

  /* Whether we print the message of DBException in wing::Instance. */
  bool print_exception_msg{true};

  /* Whether we strictly check if the chunk size <= size_batch. */
  bool check_chunk_size{false};

  /* For DEBUG: print plan and statement */
  bool debug_print_plan{false};

  /* Storage backend: options are 'memory', 'b+tree' and 'lsm' */
  std::string storage_backend_name{"memory"};

  size_t buf_pool_max_page{1024};

  /* Create a database if the file path is empty*/
  bool create_if_missing{true};

  /* Options for LSM tree */
  lsm::Options lsm_options;

  /* Options for executors */
  ExecOptions exec_options;

  /* Options of optimizers. */
  OptimizerOptions optimizer_options;
};

}  // namespace wing