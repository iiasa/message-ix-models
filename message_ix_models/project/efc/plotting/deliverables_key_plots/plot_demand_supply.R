# Net-zero sector fuels: China, stacked bars at 2030 / 2060 / 2090.
# Layout: hydrogen (large, left) | methanol + ammonia (right, stacked).
#
# One-click from this folder. Reads data/in_sector_*.csv (stack layer).
# Optional companion *_spots.csv (non-fossil production share, %) from
# vet_ribbon_generic.R draws as points on a secondary axis when present.

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

panel_specs <- list(
  list(stem = "in_sector_hydrogen_mt",
       title = "Hydrogen use by sector",
       short = "Hydrogen"),
  list(stem = "in_sector_methanol_mt",
       title = "Methanol use by sector",
       short = "Methanol"),
  list(stem = "in_sector_ammonia_mt",
       title = "Ammonia use by sector",
       short = "Ammonia")
)

year_levels <- c(2030, 2060, 2090)

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

pretty_layer <- function(x) {
  x <- gsub("_", " ", x)
  pretty_pipe <- function(s) gsub("\\s*\\|\\s*", " | ", s)
  pretty_pipe(x)
}

load_stack <- function(stem) {
  path <- file.path(data_dir, paste0(stem, ".csv"))
  if (!file.exists(path)) stop("Missing stack CSV: ", path)
  df <- read_csv(path, show_col_types = FALSE)
  if ("Layer_Order" %in% names(df)) {
    levels <- df %>% arrange(Layer_Order) %>% distinct(Layer) %>% pull(Layer)
  } else {
    levels <- sort(unique(as.character(df$Layer)))
  }
  df %>%
    filter(Year %in% year_levels) %>%
    mutate(
      Year = factor(Year, levels = year_levels),
      Layer = factor(as.character(Layer), levels = levels)
    )
}

load_spots <- function(stem) {
  path <- file.path(data_dir, paste0(stem, "_spots.csv"))
  if (!file.exists(path)) return(NULL)
  sp <- read_csv(path, show_col_types = FALSE)
  if (!"Spot_Value" %in% names(sp) || !"Year" %in% names(sp)) return(NULL)
  sp %>%
    filter(Year %in% year_levels) %>%
    mutate(Year = factor(Year, levels = year_levels))
}

if (!dir.exists(data_dir) || length(list.files(data_dir, "\\.csv$")) == 0) {
  stop("No CSVs in ", data_dir,
       ".\nRefresh with: VET_EXPORT_DELIVERABLES_ONLY=1 Rscript ../vet_ribbon_generic.R")
}

# Union of layers across panels so one shared legend / color map
all_layers <- character(0)
stacks <- list()
spots <- list()
for (spec in panel_specs) {
  stacks[[spec$stem]] <- load_stack(spec$stem)
  spots[[spec$stem]] <- load_spots(spec$stem)
  all_layers <- union(all_layers, levels(stacks[[spec$stem]]$Layer))
}
# Preserve a stable order: first appearance across H2 -> methanol -> ammonia
ordered_layers <- character(0)
for (spec in panel_specs) {
  ordered_layers <- union(ordered_layers, levels(stacks[[spec$stem]]$Layer))
}
fill_colors <- make_fill_colors(ordered_layers)
fill_labels <- setNames(pretty_layer(ordered_layers), ordered_layers)

# 2 rows across the page (~7 items each for 13 sectors).
legend_nrow <- 2L
legend_ncol <- as.integer(ceiling(length(ordered_layers) / legend_nrow))

plot_stack_bars <- function(df, spots_df, panel_title, y_label) {
  df <- df %>% mutate(Layer = factor(as.character(Layer), levels = ordered_layers))
  # Primary y max from stacked totals (same as geom_col sum); used to map
  # share-% spots onto the Mt axis for a secondary-axis overlay.
  stack_max <- df %>%
    group_by(Year) %>%
    summarise(tot = sum(Value, na.rm = TRUE), .groups = "drop") %>%
    pull(tot) %>%
    max(na.rm = TRUE)
  if (!is.finite(stack_max) || stack_max <= 0) stack_max <- 1
  y_top <- stack_max * 1.08

  has_spots <- !is.null(spots_df) && nrow(spots_df) > 0
  if (has_spots) {
    # Spot_Value is percent 0-100; map onto primary axis for sec_axis.
    spots_df <- spots_df %>%
      mutate(y_plot = Spot_Value / 100 * y_top)
  }

  p <- ggplot(df, aes(x = Year, y = Value, fill = Layer)) +
    geom_col(position = "stack", width = 0.72, color = NA, alpha = 0.95) +
    scale_fill_manual(values = fill_colors, labels = fill_labels,
                      name = NULL, drop = FALSE, limits = ordered_layers) +
    labs(title = panel_title, x = NULL, y = y_label) +
    theme_bw(base_size = 12) +
    theme(
      plot.title = element_text(size = 13, face = "bold", hjust = 0.5),
      axis.title.y = element_text(size = 11),
      axis.title.y.right = element_text(size = 10, color = "grey30"),
      axis.text = element_text(size = 11, color = "grey20"),
      axis.text.y.right = element_text(size = 9, color = "grey40"),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_blank(),
      legend.position = "none",
      plot.margin = margin(6, 10, 4, 8)
    )

  if (has_spots) {
    p <- p +
      geom_line(
        data = spots_df,
        aes(x = Year, y = y_plot, group = 1),
        inherit.aes = FALSE,
        color = "grey20", linewidth = 0.6, linetype = "22"
      ) +
      geom_point(
        data = spots_df,
        aes(x = Year, y = y_plot),
        inherit.aes = FALSE,
        shape = 21, size = 3.2, stroke = 1.1,
        fill = "white", color = "grey15"
      ) +
      scale_y_continuous(
        expand = expansion(mult = c(0, 0)),
        limits = c(0, y_top),
        labels = label_number(accuracy = NULL, big.mark = ","),
        sec.axis = sec_axis(
          ~ . / y_top * 100,
          name = "Non-fossil prod. %",
          breaks = seq(0, 100, by = 25)
        )
      )
  } else {
    p <- p +
      scale_y_continuous(
        expand = expansion(mult = c(0, 0.08)),
        labels = label_number(accuracy = NULL, big.mark = ",")
      )
  }
  p
}

# Hand-drawn legend: one swatch + label per sector.
# Absolute row heights keep the two rows tight (relative heights stretch apart).
make_legend_grob <- function() {
  n <- length(ordered_layers)
  ncol <- legend_ncol
  nrow <- legend_nrow
  # ~11" page / 7 cols; keep label width modest so cells sit closer.
  label_cm <- 3.0
  row_cm <- 0.42

  grobs <- vector("list", n)
  for (i in seq_len(n)) {
    layer <- ordered_layers[[i]]
    swatch <- rectGrob(
      width = unit(0.32, "cm"), height = unit(0.32, "cm"),
      gp = gpar(fill = fill_colors[[layer]], col = NA)
    )
    lab <- textGrob(
      fill_labels[[layer]],
      x = 0, hjust = 0, gp = gpar(fontsize = 8.5, col = "grey15")
    )
    grobs[[i]] <- arrangeGrob(
      swatch, lab, ncol = 2,
      widths = unit(c(0.42, label_cm), "cm"),
      heights = unit(row_cm, "cm")
    )
  }
  while (length(grobs) < nrow * ncol) {
    grobs[[length(grobs) + 1L]] <- nullGrob()
  }
  arrangeGrob(
    grobs = grobs, ncol = ncol, nrow = nrow,
    heights = unit(rep(row_cm, nrow), "cm"),
    padding = unit(0.8, "mm")
  )
}

unit_of <- function(df) {
  u <- unique(df$Unit)
  u <- u[!is.na(u) & u != ""][1]
  if (is.na(u) || length(u) == 0) "Mt" else u
}

panels <- list()
for (spec in panel_specs) {
  df <- stacks[[spec$stem]]
  panels[[spec$stem]] <- plot_stack_bars(
    df, spots[[spec$stem]], spec$title, unit_of(df)
  )
}

region_lab <- unique(stacks[[panel_specs[[1]]$stem]]$Region)
region_lab <- region_lab[!is.na(region_lab)][1]
if (toupper(as.character(region_lab)) %in% c("CHN", "R12_CHN")) region_lab <- "China"

scen_lab <- unique(stacks[[panel_specs[[1]]$stem]]$Scenario)
scen_lab <- scen_lab[!is.na(scen_lab)][1]
if (grepl("nz2060", scen_lab, ignore.case = TRUE)) scen_lab <- "Net-zero 2060"

top_title <- textGrob(
  paste0("Sector fuel use  |  ", region_lab, "  |  ", scen_lab),
  gp = gpar(fontsize = 15, fontface = "bold")
)

right_col <- arrangeGrob(
  panels[["in_sector_methanol_mt"]],
  panels[["in_sector_ammonia_mt"]],
  ncol = 1, heights = c(1, 1)
)
body <- arrangeGrob(
  panels[["in_sector_hydrogen_mt"]],
  right_col,
  ncol = 2, widths = c(1.35, 1)
)
legend_grob <- make_legend_grob()

has_any_spots <- any(vapply(spots, function(sp) {
  !is.null(sp) && nrow(sp) > 0
}, logical(1)))
spot_note <- if (has_any_spots) {
  textGrob(
    "o  Non-fossil production share (right axis, %)",
    gp = gpar(fontsize = 9, col = "grey25"),
    x = 0.02, hjust = 0
  )
} else {
  nullGrob()
}

# Absolute inch heights. Legend band is only slightly taller than the
# packed 2-row grob so rows are not stretched apart.
title_in <- 0.45
legend_in <- 0.22 + 0.20 * legend_nrow   # ~0.62" for 2 rows
spot_note_in <- if (has_any_spots) 0.25 else 0.0
body_in <- 5.0
fig_height <- title_in + body_in + legend_in + spot_note_in

fig <- arrangeGrob(
  top_title, body, legend_grob, spot_note,
  ncol = 1,
  heights = unit(c(title_in, body_in, legend_in, spot_note_in), "in")
)

timestamp <- format(Sys.time(), "%Y%m%d_%H%M")
pdf_path <- file.path(output_dir, "demand_supply.pdf")
png_path <- file.path(output_dir, "demand_supply.png")
stamp_path <- file.path(output_dir, paste0("demand_supply_", timestamp, ".pdf"))

cat("Layers in legend (", length(ordered_layers), "):",
    paste(ordered_layers, collapse = ", "), "\n")
cat("Legend rows:", legend_nrow, "| figure height:", fig_height, "in\n")

pdf(pdf_path, width = 11, height = fig_height)
grid.draw(fig)
dev.off()

png(png_path, width = 11, height = fig_height, units = "in", res = 200)
grid.draw(fig)
dev.off()

file.copy(pdf_path, stamp_path, overwrite = TRUE)

cat("Wrote", basename(pdf_path), "&", basename(png_path), "\n")
cat("Done. Outputs in", output_dir, "\n")
