import pandas as pd
import numpy as np
import sys

def main():
    try:
        df = pd.read_csv('grid_results.csv')
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        sys.exit(1)
        
    # Clean up "cooldown" string (e.g. "30s", "1m0s")
    if 'cooldown' in df.columns:
        df['cooldown'] = df['cooldown'].astype(str).str.replace('s', '', regex=False)
        def parse_duration(val):
            val = val.strip()
            if 'm' in val:
                parts = val.split('m')
                return float(parts[0]) * 60 + float(parts[1] if parts[1] else 0)
            return float(val)
        df['cooldown'] = df['cooldown'].apply(parse_duration)
        
    # Ensure boolean is float
    if 'use_swap' in df.columns:
        df['use_swap'] = df['use_swap'].astype(float)

    params = ['cooldown', 'move_budget', 'upper_band', 'lower_band', 'severe_ratio', 'use_swap']
    metrics = ['total_moves', 'worst_cv_reported', 'worst_mm_reported']
    
    print("=== Parameter Sensitivity (Correlation/Impact) ===")
    print("Values represent how strongly changing the parameter impacts the metric.")
    print("High absolute values indicate strong sensitivity. Values near 0 indicate no effect.\n")
    
    for metric in metrics:
        if metric not in df.columns:
            continue
        print(f"--- Target Metric: {metric} ---")
        
        # Method 1: Correlation (linear effect)
        try:
            correlations = df[params + [metric]].corr(method='spearman')[metric]
            
            # Method 2: Mean Group Range (non-linear effect)
            # Find the standard deviation of exactly how much the MEAN of the metric changes 
            # across the different values of a parameter.
            impacts = {}
            for param in params:
                grouped_means = df.groupby(param)[metric].mean()
                param_impact = grouped_means.max() - grouped_means.min()
                impacts[param] = param_impact

            # Sort and print
            print(f"{'Parameter':<15} | {'Correlation':<15} | {'Max Avg Delta':<15}")
            print("-" * 50)
            
            for param in params:
                corr = correlations[param]
                if np.isnan(corr):
                    corr = 0.0
                delta = impacts[param]
                print(f"{param:<15} | {corr:>15.4f} | {delta:>15.4f}")
        except Exception as e:
            print(f"Error calculating stats: {e}")
            
        print("\n")

if __name__ == '__main__':
    main()
