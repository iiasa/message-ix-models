# Example of an R 'module'
#
# This is a library of functions retrieved through
# message_ix_models.util.get_r_func()

add <- function(x, y) {
  return(x + y)
}

mul <- function(x, y) {
  return(x * y)
}

get_df <- function(x = 1.0) {
  return (
    data.frame(
      node_loc = c("AT", "CA"),
      value = x * c(1.2, 3.4)
    )
  )
}
