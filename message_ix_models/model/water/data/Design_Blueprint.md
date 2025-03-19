# design blueprint for water demand model refactoring

this document outlines a proposed design blueprint to improve our water demand model codebase. the goal is to transition from monolithic, hardcoded routines toward a more modular, functional, and declarative style while keeping the external function signatures intact.

---

## 1. context and challenges

- **data ingestion & transformation complexity**  
  the current implementation mixes csv reading, data-frame pivoting, interpolation, and unit conversions in one large routine. this makes it hard to test and change pieces without introducing regressions.

- **hardcoded values and duplicated logic**  
  similar code blocks (e.g., conversion factors like \(1\times10^{-3}\), the 0.95 buffer factor, or specific year adjustments) appear repeatedly. this increases the risk of inconsistencies when changes are needed.

- **scenario and policy branching**  
  depending on context (baseline, sdg, or other policies), the code branches in multiple places. this branching is currently managed with nested if/else statements rather than declarative constructs.

- **maintenance and testing difficulties**  
  impure functions with side effects coupled with the heavy use of mutable dataframes make automated testing and debugging challenging.

---

## 2. core design patterns

### 2.1. functional transformation pipeline

- **pure functions**  
  extract all independent data transformations into small, composable, and pure functions.  
  *example:* a function that takes a dataframe, applies a conversion factor, and returns a new dataframe.

- **pipeline composition**  
  use function composition, so transformations can be chained. libraries like `toolz` or simple lambda composition can help in creating a clear pipeline:
  \[
  df \rightarrow f_1(df) \rightarrow f_2(df) \rightarrow f_3(df)
  \]
  the idea is to decouple the *what* (data operations) from the *how* (order and orchestration).

### 2.2. configuration and dependency injection

- **separation of magic numbers**  
  rather than hardcoding values (e.g. \(1e{-3}\) or specific year adjustments), externalize them into configuration maps or constants. this makes the components more generic and adjustable without digging into function internals.

- **injection of behavior**  
  pass external configuration (or transformation parameters) into the pure functions so that behavior can be modified via dependency injection rather than internal branching.

### 2.3. structural pattern matching

- **dispatch via match-case**  
  to handle different scenarios (like baseline vs. sdg policies or different water sectors), leverage python’s pattern matching (introduced in python 3.10). this approach replaces nested if/else chains with a clear, declarative dispatching mechanism.

  *example:*
  ```python
  def transformation_for_sector(variable: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
      match variable:
          case "urban_withdrawal2_baseline":
              return lambda df: df.assign(value=df.value * config["urban_conversion"])
          case "rural_withdrawal_baseline":
              return lambda df: df.assign(value=df.value * config["rural_conversion"])
          case _:
              return lambda df: df  # identity transformation
  ```
  this pattern makes it trivial to add new sectors or scenarios in the future.

### 2.4. adapter / facade pattern

- **maintaining external contracts**  
  although the internal implementation evolves, the public API (e.g. `add_water_availability(context)`) must remain unchanged. an adapter or facade layer can connect composed functional pipelines with the legacy signature, ensuring backward compatibility.

- **wrapping pipelines**  
  the facade wraps the composed, functional operations and maps the final output into the expected format.

### 2.5. domain-specific language (dsl)

- **expressive transformation rules**  
  consider developing a mini-dsl or a set of declarative rules that describe the transformation for each variable or sector. this would make the handling of multiple similar routines (e.g. scaling, renaming, filtering for urban, rural, and industrial sectors) more maintainable.
  
- **rule-based mapping**  
  for instance, rules for converting units, adjusting historical values, or applying specific buffer factors can be expressed in a lightweight configuration that feeds into the transformation engine.

---

## 3. implementation blueprint

### 3.1. establish a module for pure functions and utilities

- **create a new file** (if file length permits, otherwise within the existing module) called `data_transformers.py` to hold:
  - pure transformation functions (e.g., `apply_conversion(df, factor)`, `reset_and_set_index(df)`)
  - a pipeline builder that composes these functions using a `pipe` mechanism

### 3.2. refactor branching logic using pattern matching

- **conversion functions via pattern matching**  
  centralize the case-by-case logic into functions that dispatch on a sector, variable name, or scenario, reducing widespread if/else statements.

### 3.3. isolate configuration constants

- **external configuration file or dictionary**  
  create a file (or a section in an existing config file) for parameters like:
  ```python
  # config.py
  config = {
      "urban_conversion": 1e-3,
      "rural_conversion": 1e-3,
      "buffer_factor": 0.95,
      "historical_years": [2010, 2015],
      # additional values as needed
  }
  ```

### 3.4. build wrapper / facade functions

- **facade functions**  
  within each public function (e.g. `add_irrigation_demand(context)`), wrap and compose the pure functions to produce the final demand dataframe. ensure the output structure is the same as before, even though the internal implementation is modular.

### 3.5. testing and composition

- **unit tests for pure functions**  
  ensure each small function is unit tested in isolation. this improves maintainability and confidence in the overall pipeline.
  
- **integration tests on the facade**  
  maintain integration tests that exercise the public API, so any refactoring does not break expected behavior.

---

## 4. summary

the proposed design blueprint emphasizes:
- **functional composition** to break down complex transformations into pure functions,
- **configuration management** to remove hardcoded values,
- **pattern matching** to handle branching logic declaratively,
- **adapter/facade** to maintain external interfaces,
- and the use of a **domain-specific DSL** to express transformation rules.

this design addresses our context by making the code more readable, testable, and extendable while keeping legacy function signatures intact. it also reduces duplication and centralizes changes, which is critical in a dynamic, policy-driven domain like water demand modeling.

---