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

# Retrieve the variable groups from the ribbon config
var_config <- read_yaml(file.path(script_dir, "variable_ribbon.yaml"))

if (is.null(var_config$groups) || length(var_config$groups) == 0) {
  stop("No 'groups' defined in variable_ribbon.yaml")
}

# Normalize the groups into a list of list(title=..., variables=..., aggregate_by=..., dim_s=...)
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
  list(title = title, variables = vars, aggregate_by = agg, dim_s = dim_s)
})
cat("Loaded", length(groups), "variable group(s) from config\n")

# Map each group variable to its group's dim_s (first match wins on conflict)
var_dim_s <- character(0)
for (g in groups) {
  if (!is.na(g$dim_s)) {
    for (v in g$variables) {
      if (!(v %in% names(var_dim_s))) var_dim_s[[v]] <- g$dim_s
    }
  }
}

# All variables across all groups (used to filter the data)
all_group_vars <- unique(unlist(lapply(groups, function(g) g$variables)))

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

# Dimension columns. These describe the pipe-delimited variable name:
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

# Add a dim_s (service-demand sector) column from the YAML group tags, so it
# can also be used as an aggregate_by dimension.
if (length(var_dim_s) > 0) {
  df_all$dim_s <- unname(var_dim_s[df_all$Variable])
  cat("  Tagged dim_s for", sum(!is.na(df_all$dim_s)), "rows from YAML group tags\n")
}

# Convert from wide to long format
cat("Converting from wide to long format...\n")
flush.console()
key_columns <- c("Model", "Scenario", "Region", "Variable", "Unit")
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
  mutate(Year = as.numeric(Year))
cat("  \u2713 Converted to long format:", nrow(df_long), "rows\n")
flush.console()

# Validate which group variables actually exist in the data
all_available_vars <- unique(df_long$Variable)
missing_vars <- setdiff(all_group_vars, all_available_vars)
if (length(missing_vars) > 0) {
  cat("Warning: The following variables are not found in the data:\n")
  cat("  ", paste(missing_vars, collapse = "\n  "), "\n")
}

# Filter for the selected group variables
df_long <- df_long %>%
  filter(Variable %in% all_group_vars)

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

# Optional near-term backfill: copy the configured near-term year values (e.g.
# 2020/2025), per variable and region, from a source scenario (e.g. "baseline")
# into a target scenario (e.g. "1p5c") that does not report them. Scenarios are
# matched by case-sensitive substring on the "Scenario" column. Configured via
# the YAML "near_term_fill:" block.
nt_cfg <- var_config$near_term_fill
if (!is.null(nt_cfg)) {
  from_pat <- nt_cfg$from_scenario
  to_pat <- nt_cfg$to_scenario
  fill_years <- if (!is.null(nt_cfg$years)) as.numeric(nt_cfg$years) else c(2020, 2025)
  if (is.null(from_pat) || is.null(to_pat)) {
    cat("near_term_fill: need both from_scenario and to_scenario; skipping\n")
  } else if (!"Scenario" %in% names(df_long)) {
    cat("near_term_fill: no 'Scenario' column in data; skipping\n")
  } else {
    # Source values keyed by variable+region+year (one source scenario assumed).
    src_lookup <- df_long %>%
      filter(grepl(from_pat, Scenario, fixed = TRUE), Year %in% fill_years) %>%
      group_by(Variable, Region, Year) %>%
      summarise(src_value = dplyr::first(Value[!is.na(Value)]), .groups = "drop")

    df_long <- df_long %>%
      left_join(src_lookup, by = c("Variable", "Region", "Year")) %>%
      mutate(Value = ifelse(
        grepl(to_pat, Scenario, fixed = TRUE) & Year %in% fill_years & !is.na(src_value),
        src_value, Value)) %>%
      select(-src_value)

    n_filled <- df_long %>%
      filter(grepl(to_pat, Scenario, fixed = TRUE), Year %in% fill_years, !is.na(Value)) %>%
      nrow()
    cat("near_term_fill: copied", nrow(src_lookup), "source value(s) into",
        n_filled, "target row(s) (", from_pat, "->", to_pat,
        "for years", paste(fill_years, collapse = "/"), ")\n")
    flush.console()
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
# accounting level (|final, |useful, |export) for readability.
short_label <- function(v) {
  lab <- sub("^(in|out)\\|", "", v)
  lab <- sub("\\|(final|useful|export)$", "", lab)
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
  fill_limits <- function(x) intersect(x, c(names(fill_colors), NA)) %||% character()

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
  if (length(units) >= 1) {
    unit_str <- units[1]
  } else if (has_material && has_energy) {
    unit_str <- "Mix Mt"
  } else if (has_material) {
    unit_str <- "Mt"
  } else if (has_energy) {
    unit_str <- "GWa"
  }
  unit_value <- if (unit_str != "") paste0(" (", unit_str, ")") else ""

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

# Create timestamp for filename (YYYYMMDD_HHMM format)
timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
pdf_filename <- paste0("vet_ribbon_", timestamp, ".pdf")

# Create PDF: one page per (group x scenario)
pdf(file.path(output_dir, pdf_filename), width = 16, height = 10)

total_pages <- length(groups) * length(check_scenarios)
cat("\nStarting to plot", length(groups), "group(s) x", length(check_scenarios),
    "scenario(s) =", total_pages, "pages...\n", file = stderr())
cat(paste(rep("=", 60), collapse = ""), "\n", file = stderr())
flush.console()

for (i in seq_along(groups)) {
  group <- groups[[i]]
  # Prepare once per group and derive shared y-limits across all scenarios
  prep <- prepare_group(group)
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

dev.off()

cat(paste(rep("=", 60), collapse = ""), "\n", file = stderr())
cat("All ribbon plots saved to", file.path(output_dir, pdf_filename), "\n", file = stderr())
flush.console()
