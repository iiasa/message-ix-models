library(readr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(readxl)
library(yaml)
library(gridExtra)
library(scales)
rm(list = ls())

# Locate this script so the ribbon config can live next to it regardless of
# the caller's working directory. Rscript exposes the path via --file=; when
# source()d instead, set the working directory to this folder first.
script_dir <- (function() {
  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg) == 1) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg))))
  }
  getwd()
})()

# Input/output locations are the caller's, not the repo's: point VET_DATA_DIR
# at a directory holding a check/ subdir of workflow report .xlsx files (and
# optionally source/), VET_OUTPUT_DIR at where the PDF pages should go.
data_dir <- Sys.getenv("VET_DATA_DIR", "data/vet/")
output_dir <- Sys.getenv("VET_OUTPUT_DIR", "output/vet/")

# Create output directory if it does not exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Retrieve the variable groups from the generic ribbon config.
# Names follow the clean generic_flow layout:
#   dim_head | dim_t | dim_m | dim_c | dim_l
# e.g. in|meth_h2|feedstock_dac|dac_co2|final
config_file <- file.path(script_dir, "variable_ribbon_generic.yaml")
var_config <- read_yaml(config_file)

if (is.null(var_config$groups) || length(var_config$groups) == 0) {
  stop("No 'groups' defined in variable_ribbon_generic.yaml")
}

# Optional group-level dim_head (in|out). When set, variables listed without a
# leading in|/out| get that head prepended here — sector tags stay in dim_s,
# not jammed into the variable string.
apply_dim_head <- function(vars, dim_head) {
  if (is.na(dim_head) || dim_head == "") return(vars)
  ifelse(grepl("^(in|out)\\|", vars), vars, paste0(dim_head, "|", vars))
}

# Normalize the groups into a list of list(title=..., variables=..., ...)
groups <- lapply(seq_along(var_config$groups), function(i) {
  g <- var_config$groups[[i]]
  vars <- g$variables
  vars <- vars[!is.null(vars) & vars != "" & !is.na(vars)]
  title <- if (is.null(g$title) || g$title == "") paste("Group", i) else g$title
  # Optional dimension to aggregate over (e.g. dim_c). When set, the stacked
  # layers become the distinct levels of that dimension (subtotals).
  agg <- if (is.null(g$aggregate_by) || g$aggregate_by == "") NA_character_ else g$aggregate_by
  # Optional service-demand sector tag applied to all variables in the group.
  dim_s <- if (is.null(g$dim_s) || g$dim_s == "") NA_character_ else g$dim_s
  dim_head <- if (is.null(g$dim_head) || g$dim_head == "") NA_character_ else as.character(g$dim_head)
  vars <- apply_dim_head(vars, dim_head)
  # Optional plot-time value rescale (e.g. GWa -> Mt). Does not alter source
  # values. scale_from_unit (optional regex) limits the rescale to rows whose
  # Unit matches, so energy and material rows can share one stack.
  scale_factor <- if (is.null(g$scale_factor) || is.na(g$scale_factor)) NA_real_ else as.numeric(g$scale_factor)
  plot_unit <- if (is.null(g$plot_unit) || g$plot_unit == "") NA_character_ else as.character(g$plot_unit)
  scale_from_unit <- if (is.null(g$scale_from_unit) || g$scale_from_unit == "") {
    NA_character_
  } else {
    as.character(g$scale_from_unit)
  }
  # Optional pre-scale bridge (e.g. Mt NH3 -> GWa) before scale_factor.
  # Each entry: {from: "Mt", to: "GWa", factor: 0.5882}
  unit_bridge <- g$unit_bridge
  if (is.null(unit_bridge)) unit_bridge <- list()
  # Flag groups whose plot-ready series should be written to
  # deliverables_key_plots/data/ for the key-plot polish script.
  deliverable <- isTRUE(g$deliverable)
  # Optional export filters so the deliverables folder only holds numbers
  # that appear in the polished figure (years / scenario / subfolder).
  deliverable_set <- if (is.null(g$deliverable_set) || g$deliverable_set == "") {
    NA_character_
  } else {
    as.character(g$deliverable_set)
  }
  deliverable_scenario <- if (is.null(g$deliverable_scenario) || g$deliverable_scenario == "") {
    NA_character_
  } else {
    as.character(g$deliverable_scenario)
  }
  deliverable_years <- if (is.null(g$deliverable_years) || length(g$deliverable_years) == 0) {
    numeric(0)
  } else {
    as.numeric(unlist(g$deliverable_years))
  }
  list(title = title, variables = vars, aggregate_by = agg, dim_s = dim_s,
       dim_head = dim_head, scale_factor = scale_factor, plot_unit = plot_unit,
       scale_from_unit = scale_from_unit, unit_bridge = unit_bridge,
       deliverable = deliverable, deliverable_set = deliverable_set,
       deliverable_scenario = deliverable_scenario,
       deliverable_years = deliverable_years)
})
cat("Loaded", length(groups), "variable group(s) from", basename(config_file), "\n")
n_deliverable <- sum(vapply(groups, function(g) isTRUE(g$deliverable), logical(1)))
if (n_deliverable > 0) {
  cat("  ", n_deliverable, "group(s) flagged deliverable -> CSV export\n")
}

# Expand a pipe-delimited pattern against available variable names. A segment
# of "*" matches any non-empty segment (one mode, one commodity, ...). Exact
# names (no "*") pass through unchanged so missing-var warnings still fire.
expand_var_pattern <- function(pattern, available) {
  if (!grepl("*", pattern, fixed = TRUE)) return(pattern)
  segs <- strsplit(pattern, "\\|")[[1]]
  rx_segs <- vapply(segs, function(s) {
    if (s == "*") {
      "[^|]+"
    } else {
      gsub("([][{}()+^$?|\\\\.])", "\\\\\\1", s)
    }
  }, character(1))
  rx <- paste0("^", paste(rx_segs, collapse = "\\|"), "$")
  hits <- grep(rx, available, value = TRUE)
  if (length(hits) == 0) pattern else hits
}

expand_group_vars <- function(vars, available) {
  unique(unlist(lapply(vars, expand_var_pattern, available = available),
                use.names = FALSE))
}

# Optional user-specified colors for stacked layers (named: level -> hex)
user_colors <- if (!is.null(var_config$colors)) unlist(var_config$colors) else character(0)
if (length(user_colors) > 0) {
  cat("Loaded", length(user_colors), "user-specified layer color(s)\n")
}

# Get x-axis range from config (null means use full data range)
x_axis_range <- var_config$x_axis_range
if (!is.null(x_axis_range) && length(x_axis_range) == 2) {
  cat("X-axis range set to:", x_axis_range[1], "to", x_axis_range[2], "\n")
} else {
  cat("X-axis range: using full data range\n")
}

# Function to read and combine all xlsx files from a directory
read_and_combine_xlsx <- function(dir_path, scenario_type, allow_empty = FALSE) {
  xlsx_files <- list.files(dir_path, pattern = "\\.xlsx$", full.names = TRUE)
  # Ignore temporary Excel lock files (e.g. "~$file.xlsx") created when open
  xlsx_files <- xlsx_files[!grepl("^~\\$", basename(xlsx_files))]

  if (length(xlsx_files) == 0) {
    if (allow_empty) {
      cat("No xlsx files found in", dir_path, "- skipping\n")
      return(NULL)
    } else {
      stop(paste("No xlsx files found in", dir_path))
    }
  }

  df_list <- list()
  for (file in xlsx_files) {
    df <- read_excel(file)
    # Extract scenario name from filename (remove path and extension)
    scenario_name <- gsub(".*/", "", file)
    scenario_name <- gsub("\\.xlsx$", "", scenario_name)
    df$Scenario_Type <- scenario_type
    df$Scenario_Name <- scenario_name
    df_list[[length(df_list) + 1]] <- df
  }

  # Combine all dataframes
  df_combined <- bind_rows(df_list)
  return(df_combined)
}

# Read and combine check scenarios (one area page per scenario)
cat("Reading check scenarios from", file.path(data_dir, "check"), "...\n")
flush.console()
df_check <- read_and_combine_xlsx(file.path(data_dir, "check"), "check")
cat("  \u2713 Read", nrow(df_check), "rows from check scenarios\n")
flush.console()

df_all <- df_check

# Standardize key column names. Some source files use lowercase headers
# (model/scenario/region/variable) and omit the Unit column entirely.
standardize_columns <- function(df) {
  rename_map <- c(model = "Model", scenario = "Scenario", region = "Region",
                  variable = "Variable", unit = "Unit")
  lower <- tolower(names(df))
  for (i in seq_along(lower)) {
    if (lower[i] %in% names(rename_map)) {
      names(df)[i] <- rename_map[[lower[i]]]
    }
  }
  if (!"Unit" %in% names(df)) df$Unit <- ""
  df
}
df_all <- standardize_columns(df_all)

# Dimension columns. Clean generic_flow names are always five segments:
#   dim_head | dim_t | dim_m | dim_c | dim_l
# Use them directly when present in the source files; otherwise derive them by
# splitting the Variable string on "|".
dim_cols <- c("dim_head", "dim_t", "dim_m", "dim_c", "dim_l")
if (!all(dim_cols %in% names(df_all))) {
  cat("  Deriving dimension columns from variable names\n")
  parts <- strsplit(as.character(df_all$Variable), "\\|")
  get_part <- function(p, idx) if (length(p) >= idx) p[idx] else NA_character_
  for (k in seq_along(dim_cols)) {
    df_all[[dim_cols[k]]] <- vapply(parts, get_part, character(1), idx = k)
  }
}

# Keep only in|/out| flows (generic_flow export). Drop CAP, IAMC hierarchies,
# trade aggregates, and any other non-flow rows that may share the folder.
n_before <- nrow(df_all)
df_all <- df_all %>% filter(dim_head %in% c("in", "out"))
if (nrow(df_all) < n_before) {
  cat("  Kept", nrow(df_all), "in|/out| rows; dropped",
      n_before - nrow(df_all), "non-flow rows\n")
}

# generic_flow has no Unit column. Infer model-native units from dim_l so
# scale_from_unit (GWa vs Mt) and y-axis labels work the same as before.
energy_levels <- c("final", "secondary", "useful", "primary", "intermediate",
                   "desulfurized", "stocks", "refinery", "export", "import",
                   "resource", "piped-gas", "LU", "land_use", "dummy",
                   "dummy_emission", "water_supply", "wastewater")
material_levels <- c("final_material", "secondary_material", "primary_material",
                     "tertiary_material", "useful_material", "waste_material",
                     "new_scrap", "old_scrap_1", "old_scrap_2", "old_scrap_3",
                     "end_of_life", "demand", "product", "import_fs", "export_fs",
                     "captive")
need_unit <- is.na(df_all$Unit) | df_all$Unit == ""
df_all$Unit[need_unit & df_all$dim_l %in% material_levels] <- "Mt"
df_all$Unit[need_unit & df_all$dim_l %in% energy_levels] <- "GWa"
# Application-specific useful_* levels still count as energy
df_all$Unit[need_unit & grepl("^useful_", df_all$dim_l)] <- "GWa"

# Convert from wide to long format
cat("Converting from wide to long format...\n")
flush.console()
key_columns <- c("Model", "Scenario", "Region", "Variable", "Unit", dim_cols)
year_columns <- setdiff(names(df_all), key_columns)

# Convert year columns to numeric and filter
year_columns <- year_columns[sapply(df_all[year_columns], is.numeric)]
cat("  Found", length(year_columns), "year columns\n")
flush.console()

df_long <- df_all %>%
  pivot_longer(
    cols = all_of(year_columns),
    names_to = "Year",
    values_to = "Value"
  ) %>%
  mutate(Year = as.numeric(Year),
         Value = dplyr::coalesce(Value, 0))
cat("  \u2713 Converted to long format:", nrow(df_long), "rows\n")
flush.console()

# Expand YAML patterns (including "*") against the variables actually present,
# then rebuild dim_s tags and the filter set from the expanded names.
all_available_vars <- unique(df_long$Variable)
groups <- lapply(groups, function(g) {
  g$variables <- expand_group_vars(g$variables, all_available_vars)
  g
})

var_dim_s <- character(0)
for (g in groups) {
  if (!is.na(g$dim_s)) {
    for (v in g$variables) {
      if (!(v %in% names(var_dim_s))) var_dim_s[[v]] <- g$dim_s
    }
  }
}

all_group_vars <- unique(unlist(lapply(groups, function(g) g$variables)))
cat("  Expanded config to", length(all_group_vars), "concrete variable(s)\n")

# Patterns that matched nothing stay as the pattern string and show up here.
missing_vars <- setdiff(all_group_vars, all_available_vars)
if (length(missing_vars) > 0) {
  cat("Warning: The following variables/patterns are not found in the data:\n")
  cat("  ", paste(missing_vars, collapse = "\n  "), "\n")
}

# Filter for the selected group variables
df_long <- df_long %>%
  filter(Variable %in% all_group_vars)

# Attach dim_s after filtering so only plotted rows carry the sector tag.
if (length(var_dim_s) > 0) {
  df_long$dim_s <- unname(var_dim_s[df_long$Variable])
  cat("  Tagged dim_s for", sum(!is.na(df_long$dim_s)),
      "rows from YAML group tags\n")
}

# Normalize region names so variants like NAM and R12_NAM are treated as the same
normalize_region <- function(region_name) {
  region_clean <- toupper(trimws(as.character(region_name)))
  region_clean <- gsub("\\s+", "_", region_clean)

  # Collapse prefixed region sets such as R12_NAM, R11_CPA, etc.
  region_clean <- gsub("^R[0-9]+_", "", region_clean)

  # Normalize common global labels to one canonical value
  ifelse(region_clean %in% c("WORLD", "GLB", "GLOBAL"), "WORLD", region_clean)
}

df_long <- df_long %>%
  mutate(Region_Canon = normalize_region(Region))

# Near-term historical backfill: for HIST_YEARS, replace each target scenario's
# rows entirely with a copy of HIST_SOURCE_SCENARIO (same variables/regions/
# values). 
HIST_SOURCE_SCENARIO <- "chn_baseline_sf"
HIST_TARGET_PATTERN <- "nz2060"
HIST_YEARS <- c(2020, 2025)

nt_cfg <- var_config$near_term_fill
from_pat <- if (!is.null(nt_cfg) && !is.null(nt_cfg$from_scenario)) {
  nt_cfg$from_scenario
} else {
  HIST_SOURCE_SCENARIO
}
to_pat <- if (!is.null(nt_cfg) && !is.null(nt_cfg$to_scenario)) {
  nt_cfg$to_scenario
} else {
  HIST_TARGET_PATTERN
}
fill_years <- if (!is.null(nt_cfg) && !is.null(nt_cfg$years)) {
  as.numeric(nt_cfg$years)
} else {
  HIST_YEARS
}

if (is.null(from_pat) || is.null(to_pat) || length(from_pat) == 0 || length(to_pat) == 0) {
  cat("near_term_fill: need both from_scenario and to_scenario; skipping\n")
} else if (!"Scenario" %in% names(df_long)) {
  cat("near_term_fill: no 'Scenario' column in data; skipping\n")
} else {
  src_hist <- df_long %>%
    filter(grepl(from_pat, Scenario, fixed = TRUE), Year %in% fill_years)
  if (nrow(src_hist) == 0) {
    cat("near_term_fill: no source rows for", from_pat,
        "in years", paste(fill_years, collapse = "/"), "; skipping\n")
  } else {
    # Identity columns that distinguish one scenario file from another; everything
    # else (Variable, Region, Year, Value, dim_*) is taken from the source.
    id_cols <- intersect(
      c("Model", "Scenario", "Scenario_Name", "Scenario_Type"),
      names(df_long)
    )
    targets <- df_long %>%
      filter(grepl(to_pat, Scenario, fixed = TRUE)) %>%
      distinct(across(all_of(id_cols)))
    # Do not rewrite the source scenario into itself.
    targets <- targets %>%
      filter(!grepl(from_pat, Scenario, fixed = TRUE))

    if (nrow(targets) == 0) {
      cat("near_term_fill: no target scenarios matching", to_pat, "; skipping\n")
    } else {
      transplanted <- bind_rows(lapply(seq_len(nrow(targets)), function(i) {
        row <- targets[i, , drop = FALSE]
        out <- src_hist
        for (col in id_cols) out[[col]] <- row[[col]][1]
        out
      }))

      n_drop <- df_long %>%
        filter(grepl(to_pat, Scenario, fixed = TRUE),
               !grepl(from_pat, Scenario, fixed = TRUE),
               Year %in% fill_years) %>%
        nrow()
      df_long <- df_long %>%
        filter(!(grepl(to_pat, Scenario, fixed = TRUE) &
                   !grepl(from_pat, Scenario, fixed = TRUE) &
                   Year %in% fill_years)) %>%
        bind_rows(transplanted)

      cat("near_term_fill: replaced", n_drop, "target hist row(s) with",
          nrow(transplanted), "copied from", from_pat, "->",
          paste(unique(targets$Scenario), collapse = ", "),
          "for years", paste(fill_years, collapse = "/"), "\n")
      flush.console()
    }
  }
}

# Ensure a global (WORLD / R12_GLB) series exists for each variable. If the data
# already reports a global region, keep it. Otherwise build the global total by
# summing all R12_* sub-global regions. The dim_* columns are kept so that the
# per-group dimension aggregation (aggregate_by) still works downstream.
series_keys <- intersect(
  c("Model", "Scenario", "Scenario_Name", "Scenario_Type", "Variable", "Unit",
    "dim_head", "dim_t", "dim_m", "dim_c", "dim_l", "dim_s"),
  names(df_long)
)

world_series <- df_long %>%
  filter(Region_Canon == "WORLD", !is.na(Value)) %>%
  distinct(across(all_of(series_keys)))

df_world_agg <- df_long %>%
  filter(grepl("^R12_", Region), Region_Canon != "WORLD") %>%
  anti_join(world_series, by = series_keys) %>%
  group_by(across(all_of(c(series_keys, "Year")))) %>%
  summarise(Value = if (all(is.na(Value))) NA_real_ else sum(Value, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(Region = "R12_GLB", Region_Canon = "WORLD")

n_agg_series <- nrow(distinct(df_world_agg, across(all_of(series_keys))))
if (n_agg_series > 0) {
  cat("  Aggregated R12_* into R12_GLB for", n_agg_series,
      "series lacking a global region\n")
  flush.console()
  df_long <- bind_rows(df_long, df_world_agg)
}

# Get unique regions and separate main region from others
all_regions <- unique(df_long$Region_Canon)
main_region_raw <- ifelse(is.null(var_config$main_region), "CHN", var_config$main_region)
main_region <- normalize_region(main_region_raw)
if (!main_region %in% all_regions) {
  cat("Warning: main_region", main_region, "not found in data; using", all_regions[1], "\n")
  main_region <- all_regions[1]
}
other_regions <- setdiff(all_regions, main_region)
cat("Main plot region:", main_region, "| Small plots:", length(other_regions), "regions\n")

# Scenarios from the check folder (one area page per scenario)
check_scenarios <- unique(df_long$Scenario_Name[df_long$Scenario_Type == "check"])
cat("Check scenarios:", length(check_scenarios), "\n")

# Map each scenario file (Scenario_Name) to the value(s) in the "Scenario"
# column, used as the human-readable scenario label in the page title.
scenario_label_for <- function(sname) {
  if (!"Scenario" %in% names(df_long)) return(sname)
  vals <- unique(df_long$Scenario[df_long$Scenario_Name == sname])
  vals <- vals[!is.na(vals) & vals != ""]
  if (length(vals) == 0) return(sname)
  paste(vals, collapse = " / ")
}

# Legend label: drop the leading flow direction (in|/out|) and the trailing
# accounting level (dim_l) for readability -> tech|mode|commodity.
short_label <- function(v) {
  lab <- sub("^(in|out)\\|", "", v)
  lab <- sub("\\|[^|]+$", "", lab)
  lab
}

# The "|" separator looks like a lowercase "l" in proportional fonts. Pad it
# with spaces for display so it reads clearly as a separator. (Titles/legends
# also use a monospace font where the vertical bar is unambiguous.)
pretty_pipe <- function(x) gsub("\\s*\\|\\s*", " | ", x)

# Build a color palette for a given set of stacked layers. User-specified colors
# (from the YAML "colors:" map) take priority; remaining layers get automatic
# palette colors that aren't already used by the specified ones.
make_fill_colors <- function(vars) {
  base_palette <- c("#80B1D3", "#FDB462", "#B3DE69", "#FB8072", "#BEBADA",
                    "#6A3D9A", "#e82326", "#FFED6F", "#8DD3C7", "#BC80BD",
                    "#CCEBC5", "#FCCDE5")
  cols <- setNames(rep(NA_character_, length(vars)), vars)

  # Apply user-specified colors first
  specified <- intersect(vars, names(user_colors))
  if (length(specified) > 0) {
    cols[specified] <- user_colors[specified]
  }

  # Fill the rest with palette colors not already taken
  need <- names(cols)[is.na(cols)]
  if (length(need) > 0) {
    avail <- setdiff(base_palette, cols[!is.na(cols)])
    if (length(avail) < length(need)) {
      avail <- c(avail, rainbow(length(need) - length(avail)))
    }
    cols[need] <- avail[seq_along(need)]
  }
  cols
}

# Plot a stacked-area panel for one region.
# ylim: optional c(min, max) to fix the y-axis (shared across scenarios).
plot_region_area <- function(df_region, fill_colors, fill_labels, year_breaks,
                             year_range, title, unit_value, legend_title = "Variable",
                             ylim = NULL, is_main = TRUE) {
  y_scale <- if (!is.null(ylim) && all(is.finite(ylim)) && ylim[2] > ylim[1]) {
    scale_y_continuous(limits = ylim)
  } else {
    scale_y_continuous()
  }

  # When values is a named vector, scale_fill_manual supplies its own limits
  # function that warns "No shared levels found ..." whenever the panel's fill
  # values intersect none of the names. Regions with no data for this group
  # (the R12_* panels of a bunkers group, which is reported globally only)
  # train an empty range and trip that on every such panel. Supply the same
  # limits explicitly, minus the warning; panels that do have data resolve to
  # exactly the same levels as before.
  fill_limits <- function(x) {
    out <- intersect(x, c(names(fill_colors), NA))
    if (is.null(out) || length(out) == 0) character() else out
  }

  ggplot(df_region, aes(x = Year, y = Value, fill = Layer)) +
    geom_area(position = "stack", alpha = 0.9) +
    scale_fill_manual(values = fill_colors, labels = fill_labels, name = legend_title,
                      limits = fill_limits) +
    scale_x_continuous(breaks = year_breaks, limits = year_range) +
    y_scale +
    labs(
      title = title,
      x = "Year",
      y = paste0("Value", unit_value)
    ) +
    theme_bw() +
    {
      if (is_main) {
        theme(
          plot.title = element_text(size = 16, face = "bold"),
          legend.position = "bottom",
          legend.direction = "horizontal",
          legend.key.size = unit(0.7, "cm"),
          legend.text = element_text(size = 10),
          legend.title = element_text(size = 11),
          legend.box.spacing = unit(0.5, "cm"),
          legend.margin = margin(t = 10, r = 0, b = 0, l = 0)
        )
      } else {
        theme(
          plot.title = element_text(size = 12),
          legend.position = "none",
          axis.text = element_text(size = 8),
          axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5),
          axis.title = element_text(size = 9)
        )
      }
    }
}

# Prepare a group's stacked-area data across ALL scenarios. This builds the
# "Layer" column (either per-variable or aggregated by a dimension) and filters
# to the configured x-axis year range. Returns the data plus layer metadata.
prepare_group <- function(group) {
  group_vars <- group$variables
  agg_dim <- group$aggregate_by

  df_g <- df_long %>% filter(Variable %in% group_vars)

  # Determine the y-axis unit label (dim_* columns still available here).
  # Prefer an explicit Unit column; otherwise infer from the dim_l level(s):
  #   "final"/"secondary"/"useful"/"primary"/"intermediate"/"desulfurized"/"stocks" -> energy flows in GWa
  #   "final_material"/"secondary_material"  -> material flows in Mt
  # If a group mixes energy and material levels, show "Mix Mt".
  unit_str <- ""
  units <- unique(df_g$Unit)
  units <- units[!is.na(units) & units != ""]
  dim_l_levels <- if ("dim_l" %in% names(df_g)) {
    lv <- unique(df_g$dim_l); lv[!is.na(lv) & lv != ""]
  } else character(0)
  energy_levels <- c("final", "secondary", "useful", "primary", "intermediate", "desulfurized", "stocks")
  material_levels <- c("final_material", "secondary_material", "primary_material")
  has_energy <- any(dim_l_levels %in% energy_levels)
  has_material <- any(dim_l_levels %in% material_levels)
  if (!is.na(group$plot_unit)) {
    unit_str <- group$plot_unit
  } else if (length(units) >= 1) {
    unit_str <- units[1]
  } else if (has_material && has_energy) {
    unit_str <- "Mix Mt"
  } else if (has_material) {
    unit_str <- "Mt"
  } else if (has_energy) {
    unit_str <- "GWa"
  }
  unit_value <- if (unit_str != "") paste0(" (", unit_str, ")") else ""

  # Optional unit bridge first (e.g. Mt NH3 -> GWa) so a following
  # scale_from_unit / scale_factor can treat bridged rows with the others.
  bridges <- group$unit_bridge
  if (!is.null(bridges) && length(bridges) > 0) {
    for (b in bridges) {
      from_u <- as.character(b$from)
      to_u <- as.character(b$to)
      fac <- as.numeric(b$factor)
      if (is.na(from_u) || from_u == "" || is.na(to_u) || to_u == "" || is.na(fac)) {
        next
      }
      hit <- grepl(from_u, df_g$Unit, ignore.case = TRUE)
      df_g$Value[hit] <- df_g$Value[hit] * fac
      df_g$Unit[hit] <- to_u
    }
  }

  # Plot-only rescale before aggregation so mixed-unit groups (EJ fuel + Mt
  # feedstock) can share one stack. Optional scale_from_unit regex selects
  # which rows are rescaled; others pass through. Unifying Unit to plot_unit
  # afterwards keeps aggregate_by from splitting the same dim_s layer.
  if (!is.na(group$scale_factor)) {
    if (!is.na(group$scale_from_unit)) {
      hit <- grepl(group$scale_from_unit, df_g$Unit, ignore.case = TRUE)
      df_g$Value[hit] <- df_g$Value[hit] * group$scale_factor
    } else {
      df_g <- df_g %>% mutate(Value = Value * group$scale_factor)
    }
    if (!is.na(group$plot_unit)) {
      df_g$Unit <- group$plot_unit
    }
  }

  if (!is.na(agg_dim)) {
    if (!agg_dim %in% names(df_g)) {
      stop(paste0("aggregate_by dimension '", agg_dim, "' not found in data columns"))
    }
    df_g$Layer <- as.character(df_g[[agg_dim]])
    df_g <- df_g %>%
      group_by(Scenario_Name, Region_Canon, Unit, Year, Layer) %>%
      summarise(Value = sum(Value, na.rm = TRUE), .groups = "drop")
    layer_levels <- sort(unique(df_g$Layer))
    layer_labels <- setNames(layer_levels, layer_levels)
    legend_title <- agg_dim
  } else {
    df_g$Layer <- df_g$Variable
    df_g <- df_g %>%
      select(Scenario_Name, Region_Canon, Unit, Year, Layer, Value)
    layer_levels <- unique(group_vars)
    layer_labels <- setNames(sapply(layer_levels, short_label), layer_levels)
    legend_title <- "Variable"
  }

  df_g <- df_g %>% mutate(Layer = factor(Layer, levels = layer_levels))

  # Restrict to the configured x-axis range (so shared y-limits match the plot)
  if (!is.null(x_axis_range) && length(x_axis_range) == 2) {
    df_g <- df_g %>% filter(Year >= x_axis_range[1] & Year <= x_axis_range[2])
  }

  df_g <- df_g %>%
    group_by(Scenario_Name, Region_Canon) %>%
    tidyr::complete(Year, Layer, fill = list(Value = 0)) %>%
    ungroup() %>%
    mutate(Layer = factor(Layer, levels = layer_levels))

  list(df = df_g, layer_levels = layer_levels, layer_labels = layer_labels,
       legend_title = legend_title, unit_value = unit_value)
}

# Compute a shared y-limit per region (max stacked total across all scenarios),
# so the same panel is comparable across scenario pages. Returns a named list
# keyed by Region_Canon, each c(ymin, ymax) or NULL when there is no data.
compute_region_ylims <- function(df_g) {
  totals <- df_g %>%
    group_by(Region_Canon, Scenario_Name, Year) %>%
    summarise(pos = sum(pmax(Value, 0), na.rm = TRUE),
              neg = sum(pmin(Value, 0), na.rm = TRUE), .groups = "drop") %>%
    group_by(Region_Canon) %>%
    summarise(ymax = max(pos, na.rm = TRUE), ymin = min(neg, na.rm = TRUE),
              .groups = "drop")
  ylims <- list()
  for (k in seq_len(nrow(totals))) {
    ymax <- totals$ymax[k]
    ymin <- totals$ymin[k]
    if (!is.finite(ymax) || !is.finite(ymin) || (ymax <= 0 && ymin >= 0)) {
      ylims[[totals$Region_Canon[k]]] <- NULL
    } else {
      ylims[[totals$Region_Canon[k]]] <- c(ifelse(ymin < 0, ymin * 1.05, 0),
                                           ifelse(ymax > 0, ymax * 1.05, 0))
    }
  }
  ylims
}

# Build one full page (main region big on left, others small on right) for a
# single group + single scenario, using shared per-region y-limits.
plot_group_scenario <- function(group, prep, region_ylims, scenario_name, page_label) {
  df_g <- prep$df %>% filter(Scenario_Name == scenario_name)

  unit_value <- prep$unit_value

  # Year range / breaks
  if (!is.null(x_axis_range) && length(x_axis_range) == 2) {
    year_range <- c(x_axis_range[1], x_axis_range[2])
  } else {
    year_range <- range(df_g$Year, na.rm = TRUE)
  }
  year_breaks <- seq(floor(year_range[1] / 10) * 10, ceiling(year_range[2] / 10) * 10, by = 10)

  fill_colors <- make_fill_colors(prep$layer_levels)
  fill_labels <- pretty_pipe(prep$layer_labels)
  legend_title <- prep$legend_title
  scenario_label <- scenario_label_for(scenario_name)

  # Main region (big, left)
  df_main <- df_g %>% filter(Region_Canon == main_region)
  main_title_region <- ifelse(main_region == "WORLD", "World", main_region)
  main_title <- paste0(page_label, ": ", pretty_pipe(group$title), " - ", main_title_region,
                       "\n", scenario_label)

  p_main <- plot_region_area(df_main, fill_colors, fill_labels, year_breaks,
                             year_range, main_title, unit_value, legend_title,
                             ylim = region_ylims[[main_region]], is_main = TRUE)

  # Other regions (small, right)
  if (length(other_regions) > 0) {
    plot_list <- list()
    for (region in other_regions) {
      df_region <- df_g %>% filter(Region_Canon == region)
      region_title <- ifelse(region == "WORLD", "World", region)
      p_region <- plot_region_area(df_region, fill_colors, fill_labels, year_breaks,
                                   year_range, region_title, unit_value, legend_title,
                                   ylim = region_ylims[[region]], is_main = FALSE)
      plot_list[[length(plot_list) + 1]] <- p_region
    }

    n_cols <- 3  # Number of columns for small plots
    combined_plot <- grid.arrange(
      p_main,
      do.call(arrangeGrob, c(plot_list, ncol = n_cols)),
      ncol = 2,
      widths = c(1.2, 1)  # Main region takes more width on left
    )
  } else {
    combined_plot <- p_main
  }

  return(combined_plot)
}

# ---------------------------------------------------------------------------
# Deliverable CSV export: plot-ready series for groups with deliverable: true.
# Written next to this script under deliverables_key_plots/data/ so the polish
# script can one-click re-run without re-reading the Excel reports.
# ---------------------------------------------------------------------------
slugify <- function(title) {
  s <- tolower(as.character(title))
  s <- gsub("[^a-z0-9]+", "_", s)
  gsub("^_|_$", "", s)
}

deliverables_dir <- file.path(script_dir, "deliverables_key_plots")
deliverables_data_dir <- file.path(deliverables_dir, "data")
if (!dir.exists(deliverables_data_dir)) {
  dir.create(deliverables_data_dir, recursive = TRUE)
}

# Spot overlay (second layer on the stacked bars). Computed here so the
# deliverables folder only receives finished numbers.
# Non-fossil production share (%) for sector-fuel deliverables: look up the
# matching out|… production group, drop fossil tech layers, divide by total.
# Output columns: Title/Scenario/Scenario_Name/Region/Year/Spot_Value
# (+ Spot_Label, Unit). Region is always main_region (R12_CHN -> CHN).
nonfossil_spot_specs <- list(
  "in|sector|hydrogen (Mt)" = list(
    production_title = "out|hydrogen production|by technology (Mt)",
    # Fossil: SMR, coal gasification, catalytic reforming, CO gas
    fossil_pattern = "smr|coal|catalytic_reforming|co_gas"
  ),
  "in|sector|methanol (Mt)" = list(
    production_title = "out|methanol production|by technology (Mt)",
    # Fossil: natural gas, coal (bio / h2 remain non-fossil)
    fossil_pattern = "(^|_)ng(_|$)|(^|_)coal(_|$)"
  ),
  "in|sector|ammonia (Mt)" = list(
    production_title = "out|ammonia production|by technology (Mt)",
    # Fossil: gas, coal, fuel oil (biomass / electr remain non-fossil)
    fossil_pattern = "(^|_)gas(_|$)|(^|_)coal(_|$)|fueloil"
  )
)

# Cache prepare_group() results so spot lookups reuse production preps.
prep_cache <- new.env(parent = emptyenv())

get_group_prep <- function(title) {
  if (exists(title, envir = prep_cache, inherits = FALSE)) {
    return(get(title, envir = prep_cache))
  }
  idx <- which(vapply(groups, function(g) identical(g$title, title), logical(1)))
  if (length(idx) == 0) return(NULL)
  prep <- prepare_group(groups[[idx[[1]]]])
  assign(title, prep, envir = prep_cache)
  prep
}

compute_deliverable_spots <- function(group, prep, df_stack) {
  spec <- nonfossil_spot_specs[[group$title]]
  if (is.null(spec)) return(NULL)

  prod_prep <- get_group_prep(spec$production_title)
  if (is.null(prod_prep) || is.null(prod_prep$df) || nrow(prod_prep$df) == 0) {
    cat("  Spot skip:", group$title, "- no prep for",
        spec$production_title, "\n", file = stderr())
    return(NULL)
  }

  # Match the stack export filters (scenario / years / main region).
  scens <- unique(as.character(df_stack$Scenario_Name))
  years <- unique(as.numeric(df_stack$Year))

  df_prod <- prod_prep$df %>%
    filter(Region_Canon == main_region,
           as.character(Scenario_Name) %in% scens,
           Year %in% years) %>%
    mutate(Layer = as.character(Layer),
           is_fossil = grepl(spec$fossil_pattern, Layer, ignore.case = TRUE))

  if (nrow(df_prod) == 0) {
    cat("  Spot skip:", group$title, "- no production rows for",
        main_region, "\n", file = stderr())
    return(NULL)
  }

  shares <- df_prod %>%
    group_by(Scenario_Name, Region_Canon, Year) %>%
    summarise(
      total = sum(Value, na.rm = TRUE),
      fossil = sum(Value[is_fossil], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      Spot_Value = ifelse(total > 0, 100 * (total - fossil) / total, NA_real_),
      Scenario = vapply(as.character(Scenario_Name), scenario_label_for, character(1)),
      Title = group$title,
      Region = Region_Canon,
      Spot_Label = "Non-fossil production share",
      Unit = "%"
    ) %>%
    select(Title, Scenario, Scenario_Name, Region, Year, Spot_Value,
           Spot_Label, Unit) %>%
    arrange(Scenario, Year)

  # Drop years with no production (NA share)
  shares <- shares %>% filter(!is.na(Spot_Value))
  if (nrow(shares) == 0) return(NULL)
  shares
}

export_deliverable_csv <- function(group, prep) {
  if (!isTRUE(group$deliverable)) return(invisible(NULL))

  unit_str <- sub("^ \\((.*)\\)$", "\\1", prep$unit_value)
  if (is.null(unit_str) || is.na(unit_str)) unit_str <- ""

  out_dir <- deliverables_data_dir
  if (!is.na(group$deliverable_set) && group$deliverable_set != "") {
    out_dir <- file.path(deliverables_data_dir, group$deliverable_set)
  }
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  df_out <- prep$df %>%
    filter(Region_Canon == main_region) %>%
    mutate(
      Scenario = vapply(as.character(Scenario_Name), scenario_label_for, character(1)),
      Title = group$title,
      Unit = dplyr::coalesce(na_if(as.character(Unit), ""), unit_str),
      Layer = as.character(Layer),
      Layer_Order = as.integer(match(Layer, prep$layer_levels)),
      Legend_Title = prep$legend_title,
      Region = Region_Canon
    )

  if (!is.na(group$deliverable_scenario) && group$deliverable_scenario != "") {
    pat <- group$deliverable_scenario
    df_out <- df_out %>%
      filter(grepl(pat, Scenario, ignore.case = TRUE) |
               grepl(pat, Scenario_Name, ignore.case = TRUE))
  }
  if (length(group$deliverable_years) > 0) {
    df_out <- df_out %>% filter(Year %in% group$deliverable_years)
  }

  # Drop layers that are zero in every kept year/scenario (plot-ready only)
  live_layers <- df_out %>%
    group_by(Layer) %>%
    summarise(tot = sum(abs(Value), na.rm = TRUE), .groups = "drop") %>%
    filter(tot > 0) %>%
    pull(Layer)
  df_out <- df_out %>%
    filter(Layer %in% live_layers) %>%
    mutate(Layer_Order = as.integer(match(Layer, prep$layer_levels))) %>%
    select(Title, Scenario, Scenario_Name, Region, Year, Layer, Layer_Order,
           Value, Unit, Legend_Title) %>%
    arrange(Scenario, Year, Layer_Order)

  stem <- slugify(group$title)
  out_path <- file.path(out_dir, paste0(stem, ".csv"))
  readr::write_csv(df_out, out_path)
  cat("  Deliverable CSV:", file.path(basename(out_dir), basename(out_path)),
      "(", nrow(df_out), "rows,", main_region, ")\n", file = stderr())

  spots <- compute_deliverable_spots(group, prep, df_out)
  if (!is.null(spots) && nrow(spots) > 0) {
    spots_path <- file.path(out_dir, paste0(stem, "_spots.csv"))
    readr::write_csv(spots, spots_path)
    cat("  Deliverable spots:", basename(spots_path), "(", nrow(spots),
        "rows)\n", file = stderr())
  }

  flush.console()
  invisible(out_path)
}

# Optional: VET_EXPORT_DELIVERABLES_ONLY=1 skips the vetting PDF and only
# writes the deliverable CSVs (much faster when refreshing key-plot inputs).
export_only <- Sys.getenv("VET_EXPORT_DELIVERABLES_ONLY", "") %in% c("1", "true", "TRUE")

# Create timestamp for filename (YYYYMMDD_HHMM format)
timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
pdf_filename <- paste0("vet_ribbon_", timestamp, ".pdf")

if (!export_only) {
  # Create PDF: one page per (group x scenario)
  pdf(file.path(output_dir, pdf_filename), width = 16, height = 10)
}

total_pages <- length(groups) * length(check_scenarios)
if (export_only) {
  cat("\nExport-only mode: writing deliverable CSVs (skipping PDF)...\n",
      file = stderr())
} else {
  cat("\nStarting to plot", length(groups), "group(s) x", length(check_scenarios),
      "scenario(s) =", total_pages, "pages...\n", file = stderr())
}
cat(paste(rep("=", 60), collapse = ""), "\n", file = stderr())
flush.console()

for (i in seq_along(groups)) {
  group <- groups[[i]]
  if (export_only && !isTRUE(group$deliverable)) next

  # Prepare once per group and derive shared y-limits across all scenarios
  prep <- prepare_group(group)
  assign(group$title, prep, envir = prep_cache)
  export_deliverable_csv(group, prep)

  if (export_only) next

  region_ylims <- compute_region_ylims(prep$df)
  for (j in seq_along(check_scenarios)) {
    scenario_name <- check_scenarios[j]
    page_label <- paste0(i, LETTERS[j])
    ts <- format(Sys.time(), "%H:%M:%S")
    cat(sprintf("[%s] [page %s] %s | %s\n", ts, page_label, group$title,
                scenario_label_for(scenario_name)),
        file = stderr())
    flush.console()
    print(plot_group_scenario(group, prep, region_ylims, scenario_name, page_label))
    cat(sprintf("[%s]   \u2713 Completed page %s\n", format(Sys.time(), "%H:%M:%S"), page_label),
        file = stderr())
    flush.console()
  }
}

if (!export_only) {
  dev.off()
  cat(paste(rep("=", 60), collapse = ""), "\n", file = stderr())
  cat("All ribbon plots saved to", file.path(output_dir, pdf_filename), "\n",
      file = stderr())
} else {
  cat(paste(rep("=", 60), collapse = ""), "\n", file = stderr())
  cat("Deliverable CSVs written to", deliverables_data_dir, "\n", file = stderr())
}
flush.console()
