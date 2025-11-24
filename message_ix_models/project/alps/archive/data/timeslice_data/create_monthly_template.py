"""
Generate monthly timeslice Excel template for MESSAGEix scenarios.

This script creates an Excel file with the necessary structure for adding
12 monthly timeslices to a MESSAGEix scenario.
"""

import pandas as pd

def create_monthly_template(regions="R12", output_file=None):
    """Create Excel template for 12 monthly timeslices.

    Parameters
    ----------
    regions : str
        Region specification (e.g., 'R12')
    output_file : str, optional
        Output file path
    """
    if output_file is None:
        output_file = f"input_data_12_{regions}.xlsx"

    # Create ExcelWriter object
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:

        # Sheet 1: time_steps
        # Define 12 monthly timeslices
        time_steps = pd.DataFrame({
            'time': [str(i) for i in range(1, 13)],
            'lvl_temporal': ['month'] * 12,
            'parent_time': ['year'] * 12,
            'duration_time': [1/12] * 12  # Each month is 1/12 of a year
        })
        time_steps.to_excel(writer, sheet_name='time_steps', index=False)

        # Sheet 2: capacity_factor
        # Empty template - timeslice module will use uniform distribution
        # Only include 'rate' row, no actual data columns
        cf_data = {
            'index': ['rate'],
        }
        cf_df = pd.DataFrame(cf_data)
        cf_df.to_excel(writer, sheet_name='capacity_factor', index=False)

        # Sheet 3: output
        # Empty template
        output_data = {
            'index': ['rate'],
        }
        output_df = pd.DataFrame(output_data)
        output_df.to_excel(writer, sheet_name='output', index=False)

        # Sheet 4: input
        # Empty template
        input_data = {
            'index': ['rate'],
        }
        input_df = pd.DataFrame(input_data)
        input_df.to_excel(writer, sheet_name='input', index=False)

        # Sheet 5: peak_demand
        # Template for reserve margin calculations
        # This is region-specific and would need actual data
        peak_data = {
            'year': ['peak', 2025, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100],
        }
        # Add placeholder columns for each region in R12
        # These would need to be filled with actual peak demand data
        regions_list = [f"{regions}_{i}" for i in ['AFR', 'RCPA', 'EEU', 'FSU',
                                                     'LAM', 'MEA', 'NAM', 'PAO',
                                                     'PAS', 'SAS', 'WEU', 'CPA']]
        for reg in regions_list:
            peak_data[reg] = [0.0] * 10  # Placeholder values

        peak_df = pd.DataFrame(peak_data)
        peak_df.to_excel(writer, sheet_name='peak_demand', index=False)

    print(f"Created template: {output_file}")
    print("\nNext steps:")
    print("1. Fill in technology-specific capacity factors (especially for VRE)")
    print("2. Add peak demand data for reserve margin calculations")
    print("3. Optionally add technology-specific input/output variations")

    return output_file


if __name__ == "__main__":
    import sys
    regions = sys.argv[1] if len(sys.argv) > 1 else "R12"
    create_monthly_template(regions)
