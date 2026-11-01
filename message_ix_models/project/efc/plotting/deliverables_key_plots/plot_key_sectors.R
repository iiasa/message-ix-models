# Key plots: China only, baseline (left) vs nz2060 (right).
#
# One-click: open this folder and source / Rscript plot_key_sectors.R
# (no path arguments). Reads CSVs from ./data/, writes PDF/PNG to ./output/.
#
# Refresh inputs by running the sibling vet_ribbon_generic.R with
#   VET_EXPORT_DELIVERABLES_ONLY=1
# after marking groups deliverable: true in variable_ribbon_generic.yaml.

library(readr)
library(dplyr)
library(ggplot2)
library(yaml)
library(gridExtra)
library(grid)
library(scales)
rm(list = ls())

script_dir <- (function() {
  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg) == 1) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg))))
  }
  getwd()
})()
setwd(script_dir)

data_dir <- file.path(script_dir, "data")
output_dir <- file.path(script_dir, "output")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Scenario panel order and display labels (left -> right)
scenario_specs <- list(
  list(pattern = "baseline", label = "Baseline"),
  list(pattern = "nz2060", label = "Net-zero 2060")
)

# Optional cleaner figure titles keyed by the YAML Title column in each CSV.
display_titles <- c(
  "in|shipping|by commodity (EJ)" = "Shipping energy use by fuel",
  "in|aviation|by commodity (EJ)" = "Aviation energy use by fuel",
  "in|olefins & BTX|by commodity (Mt)" = "Olefins & BTX feedstock by commodity",
  "in|iron|by commodity (EJ)" = "Iron & steel energy use by commodity",
  "in|industry other|by commodity (EJ)" = "Other industry energy use by commodity"
)

# Fixed figure order (matches the five deliverable groups). Fallback: alpha.
preferred_order <- c(
  "in_shipping_by_commodity_ej",
  "in_aviation_by_commodity_ej",
  "in_olefins_btx_by_commodity_mt",
  "in_iron_by_commodity_ej",
  "in_industry_other_by_commodity_ej"
)

colors_file <- file.path(script_dir, "colors.yaml")
user_colors <- if (file.exists(colors_file)) {
  unlist(read_yaml(colors_file))
} else {
  character(0)
}

make_fill_colors <- function(layers) {
  base_palette <- c("#80B1D3", "#FDB462", "#B3DE69", "#FB8072", "#BEBADA",
                    "#6A3D9A", "#e82326", "#FFED6F", "#8DD3C7", "#BC80BD",
                    "#CCEBC5", "#FCCDE5", "#4D4D4D", "#A65628", "#FF7F00")
  cols <- setNames(rep(NA_character_, length(layers)), layers)
  specified <- intersect(layers, names(user_colors))
  if (length(specified) > 0) cols[specified] <- user_colors[specified]
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

pretty_pipe <- function(x) gsub("\\s*\\|\\s*", " | ", x)

match_scenario <- function(df, pattern) {
  hit <- grepl(pattern, df$Scenario, ignore.case = TRUE) |
    grepl(pattern, df$Scenario_Name, ignore.case = TRUE)
  df %>% filter(hit)
}

plot_panel <- function(df_scen, fill_colors, year_range, year_breaks, ylim,
                       panel_title, y_label, show_legend = FALSE) {
  ggplot(df_scen, aes(x = Year, y = Value, fill = Layer)) +
    geom_area(position = "stack", alpha = 0.92, color = NA) +
    scale_fill_manual(values = fill_colors, name = NULL, drop = FALSE) +
    scale_x_continuous(breaks = year_breaks, limits = year_range,
                       expand = expansion(mult = c(0.01, 0.01))) +
    scale_y_continuous(limits = ylim, expand = expansion(mult = c(0, 0.02)),
                       labels = label_number(accuracy = NULL, big.mark = ",")) +
    labs(title = panel_title, x = NULL, y = y_label) +
    theme_bw(base_size = 12) +
    theme(
      plot.title = element_text(size = 13, face = "bold", hjust = 0.5),
      axis.title.y = element_text(size = 11),
      axis.text = element_text(size = 10, color = "grey20"),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_blank(),
      legend.position = if (show_legend) "bottom" else "none",
      legend.direction = "horizontal",
      legend.key.size = unit(0.55, "cm"),
      legend.text = element_text(size = 9),
      plot.margin = margin(6, 8, 4, 8)
    ) +
    guides(fill = guide_legend(nrow = 2, byrow = TRUE))
}

# Only the set-1 comparison CSVs (ignore in_sector_* / *_spots.csv sharing data/).
csv_files <- file.path(data_dir, paste0(preferred_order, ".csv"))
csv_files <- csv_files[file.exists(csv_files)]
if (length(csv_files) == 0) {
  stop("No set-1 CSVs in ", data_dir,
       ".\nRefresh with: VET_EXPORT_DELIVERABLES_ONLY=1 Rscript ../vet_ribbon_generic.R")
}

cat("Found", length(csv_files), "key-sector CSV(s) in", data_dir, "\n")

figures <- list()
timestamp <- format(Sys.time(), "%Y%m%d_%H%M")

for (csv_path in csv_files) {
  df <- read_csv(csv_path, show_col_types = FALSE)
  required <- c("Scenario", "Scenario_Name", "Year", "Layer", "Value")
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) {
    stop("CSV missing columns ", paste(missing, collapse = ", "), ": ", csv_path)
  }

  title_raw <- unique(df$Title)
  title_raw <- title_raw[!is.na(title_raw) & title_raw != ""][1]
  if (is.na(title_raw) || length(title_raw) == 0) {
    title_raw <- tools::file_path_sans_ext(basename(csv_path))
  }
  fig_title <- if (title_raw %in% names(display_titles)) {
    unname(display_titles[[title_raw]])
  } else {
    pretty_pipe(title_raw)
  }

  unit_str <- unique(df$Unit)
  unit_str <- unit_str[!is.na(unit_str) & unit_str != ""][1]
  y_label <- if (is.na(unit_str) || length(unit_str) == 0) "Value" else unit_str

  if ("Layer_Order" %in% names(df)) {
    layer_levels <- df %>%
      arrange(Layer_Order) %>%
      distinct(Layer) %>%
      pull(Layer)
  } else {
    layer_levels <- sort(unique(as.character(df$Layer)))
  }
  df <- df %>% mutate(Layer = factor(as.character(Layer), levels = layer_levels))

  year_range <- range(df$Year, na.rm = TRUE)
  year_breaks <- seq(floor(year_range[1] / 10) * 10,
                     ceiling(year_range[2] / 10) * 10, by = 10)

  # Shared y-limit across both scenarios (stacked totals)
  totals <- df %>%
    group_by(Scenario_Name, Year) %>%
    summarise(pos = sum(pmax(Value, 0), na.rm = TRUE),
              neg = sum(pmin(Value, 0), na.rm = TRUE), .groups = "drop")
  ymax <- max(totals$pos, na.rm = TRUE)
  ymin <- min(totals$neg, na.rm = TRUE)
  if (!is.finite(ymax) || ymax <= 0) ymax <- 1
  ylim <- c(if (is.finite(ymin) && ymin < 0) ymin * 1.05 else 0, ymax * 1.08)

  fill_colors <- make_fill_colors(as.character(layer_levels))

  panels <- list()
  for (spec in scenario_specs) {
    df_scen <- match_scenario(df, spec$pattern)
    if (nrow(df_scen) == 0) {
      warning("No rows matching '", spec$pattern, "' in ", basename(csv_path))
      next
    }
    # One scenario file expected; if several match, keep the first label group
    keep_name <- df_scen$Scenario_Name[1]
    df_scen <- df_scen %>% filter(Scenario_Name == keep_name)
    panels[[length(panels) + 1]] <- plot_panel(
      df_scen, fill_colors, year_range, year_breaks, ylim,
      panel_title = spec$label, y_label = y_label, show_legend = FALSE
    )
  }

  if (length(panels) == 0) {
    warning("Skipping empty figure for ", basename(csv_path))
    next
  }

  # Shared legend grob from a throwaway plot (no cowplot dependency)
  extract_legend <- function(p) {
    g <- ggplotGrob(p)
    idx <- which(vapply(g$grobs, function(x) x$name, character(1)) == "guide-box")
    if (length(idx) == 0) return(NULL)
    g$grobs[[idx[1]]]
  }
  legend_plot <- plot_panel(
    df %>% filter(Scenario_Name == df$Scenario_Name[1]),
    fill_colors, year_range, year_breaks, ylim,
    panel_title = "", y_label = y_label, show_legend = TRUE
  )
  legend_grob <- extract_legend(legend_plot)

  region_lab <- unique(df$Region)
  region_lab <- region_lab[!is.na(region_lab)][1]
  region_lab <- if (is.na(region_lab) || length(region_lab) == 0) "CHN" else region_lab
  if (toupper(region_lab) %in% c("CHN", "R12_CHN")) region_lab <- "China"

  top_title <- textGrob(
    paste0(fig_title, "  |  ", region_lab),
    gp = gpar(fontsize = 15, fontface = "bold")
  )

  body <- do.call(arrangeGrob, c(panels, list(ncol = length(panels))))
  if (!is.null(legend_grob)) {
    fig <- arrangeGrob(top_title, body, legend_grob,
                       heights = c(0.08, 0.80, 0.12))
  } else {
    fig <- arrangeGrob(top_title, body, heights = c(0.1, 0.9))
  }

  stem <- tools::file_path_sans_ext(basename(csv_path))
  pdf_path <- file.path(output_dir, paste0(stem, ".pdf"))
  png_path <- file.path(output_dir, paste0(stem, ".png"))

  pdf(pdf_path, width = 11, height = 5.2)
  grid.draw(fig)
  dev.off()

  png(png_path, width = 11, height = 5.2, units = "in", res = 200)
  grid.draw(fig)
  dev.off()

  figures[[stem]] <- fig
  cat("  Wrote", basename(pdf_path), "&", basename(png_path), "\n")
}

# Combined PDF for easy sharing
if (length(figures) > 0) {
  combo_path <- file.path(output_dir, paste0("key_figures_", timestamp, ".pdf"))
  pdf(combo_path, width = 11, height = 5.2)
  for (fig in figures) {
    grid.newpage()
    grid.draw(fig)
  }
  dev.off()
  cat("Combined PDF:", combo_path, "\n")
}

cat("Done. Outputs in", output_dir, "\n")
