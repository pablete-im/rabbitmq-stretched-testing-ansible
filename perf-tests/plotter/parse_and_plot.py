#!/usr/bin/env python3
"""
Performance Test Results Parser and Plotter

This script parses RabbitMQ performance test results and generates line graphs
for different metrics across multiple test runs.
"""

import os
import re
import glob
import argparse
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path


class PerfTestParser:
    def __init__(self, results_dir="results"):
        self.results_dir = results_dir
        self.plots_dir = os.path.join(results_dir, "plots")
        os.makedirs(self.plots_dir, exist_ok=True)
        
    def get_files_by_filter(self, filter_name, max_files=None):
        """Get files matching the filter pattern, sorted by modification time (newest first)"""
        pattern = f"*-{filter_name}.txt*"
        files = glob.glob(os.path.join(self.results_dir, pattern))
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        if max_files:
            files = files[:max_files]
            
        return files
    
    def extract_scenario_name(self, filepath):
        """Extract scenario name from the first line that starts with '# Scenario:'"""
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.startswith("# Scenario:"):
                        return line.split(":", 1)[1].strip()
                    # Stop after checking a reasonable number of lines
                    if line.strip() and not line.startswith("#"):
                        break
        except Exception as e:
            print(f"Warning: Could not extract scenario from {filepath}: {e}")
        
        # Fallback: extract from filename
        basename = os.path.basename(filepath)
        parts = basename.split('-')
        if len(parts) >= 3:
            # Remove date, timestamp, and extension
            scenario_parts = parts[2:]
            scenario = '-'.join(scenario_parts).replace('.txt', '').replace('.consumer', '')
            return scenario
        
        return "unknown"
    
    def parse_data_line(self, line):
        """Parse a data line that starts with 'id: ..., time ...'"""
        # Pattern to match the data lines
        pattern = r'id: ([^,]+), time ([\d.]+) s, (.+)'
        match = re.match(pattern, line.strip())
        
        if not match:
            return None
            
        test_id = match.group(1)
        time_seconds = float(match.group(2))
        data_part = match.group(3)
        
        result = {
            'time': round(time_seconds),  # Round to nearest integer second
            'sent': None,
            'confirmed': None,
            'received': None,
            'consumer_latency_median': None,
            'confirm_latency_median': None
        }
        
        # Parse sent messages
        sent_match = re.search(r'sent: ([\d.]+) msg/s', data_part)
        if sent_match:
            result['sent'] = float(sent_match.group(1))
            
        # Parse confirmed messages
        confirmed_match = re.search(r'confirmed: ([\d.]+) msg/s', data_part)
        if confirmed_match:
            result['confirmed'] = float(confirmed_match.group(1))
            
        # Parse received messages
        received_match = re.search(r'received: ([\d.]+) msg/s', data_part)
        if received_match:
            result['received'] = float(received_match.group(1))
            
        # Parse consumer latency (median is the 2nd value, handle both µs and ms)
        consumer_lat_match = re.search(r'consumer latency: ([\d./]+) (µs|ms)', data_part)
        if consumer_lat_match:
            latency_values = consumer_lat_match.group(1).split('/')
            unit = consumer_lat_match.group(2)
            if len(latency_values) >= 2:
                try:
                    median_value = float(latency_values[1])
                    # Convert to milliseconds if needed
                    if unit == 'µs':
                        result['consumer_latency_median'] = median_value / 1000.0
                    else:  # unit == 'ms'
                        result['consumer_latency_median'] = median_value
                except ValueError:
                    pass  # Skip invalid values
                    
        # Parse confirm latency (median is the 2nd value, handle both µs and ms)
        confirm_lat_match = re.search(r'confirm latency: ([\d./]+) (µs|ms)', data_part)
        if confirm_lat_match:
            latency_values = confirm_lat_match.group(1).split('/')
            unit = confirm_lat_match.group(2)
            if len(latency_values) >= 2:
                try:
                    median_value = float(latency_values[1])
                    # Convert to milliseconds if needed
                    if unit == 'µs':
                        result['confirm_latency_median'] = median_value / 1000.0
                    else:  # unit == 'ms'
                        result['confirm_latency_median'] = median_value
                except ValueError:
                    pass  # Skip invalid values
        
        return result
    
    def parse_file(self, filepath):
        """Parse a single performance test file"""
        scenario = self.extract_scenario_name(filepath)
        data_points = []
        
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.startswith('id:') and ', time ' in line:
                        parsed_data = self.parse_data_line(line)
                        if parsed_data:
                            data_points.append(parsed_data)
        except Exception as e:
            print(f"Error parsing file {filepath}: {e}")
            return None, []
            
        return scenario, data_points
    
    def create_plot(self, metric, data_series, filter_name):
        """Create a line plot for a specific metric"""
        plt.figure(figsize=(16, 8))  # Make it more landscape oriented
        
        colors = plt.cm.tab10(range(len(data_series)))
        
        for i, (series_name, points) in enumerate(data_series.items()):
            if not points:
                continue
            
            # Group data by time, averaging values for the same time point
            grouped_data = {}
            for point in points:
                if point[metric] is not None:
                    time_point = point['time']
                    if time_point in grouped_data:
                        # Average with existing value
                        existing_value = grouped_data[time_point]
                        grouped_data[time_point] = (existing_value + point[metric]) / 2.0
                    else:
                        grouped_data[time_point] = point[metric]
            
            if grouped_data:
                times = sorted(grouped_data.keys())
                values = [grouped_data[t] for t in times]
                plt.plot(times, values, marker='o', markersize=3, 
                        label=series_name, color=colors[i], linewidth=1.5)
        
        plt.xlabel('Time (seconds)')
        
        # Set appropriate y-label based on metric
        if 'latency' in metric:
            plt.ylabel('Latency (ms)')
        else:
            plt.ylabel('Messages per second')
            
        plt.title(f'{metric.replace("_", " ").title()} - {filter_name}')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Generate filename
        today = datetime.now().strftime('%Y%m%d')
        filename = f"{today}-{filter_name}-{metric}.png"
        filepath = os.path.join(self.plots_dir, filename)
        
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {filepath}")
        return filepath
    
    def process_files(self, filter_name, max_files=None):
        """Process all files matching the filter and generate plots"""
        files = self.get_files_by_filter(filter_name, max_files)
        
        if not files:
            print(f"No files found matching filter '{filter_name}'")
            return
            
        print(f"Processing {len(files)} files for filter '{filter_name}':")
        for f in files:
            print(f"  - {os.path.basename(f)}")
        
        # Parse all files and organize data by metric
        all_data = defaultdict(dict)
        
        # Group files by base name (without .consumer extension) to merge producer/consumer data
        file_groups = defaultdict(list)
        for filepath in files:
            basename = os.path.basename(filepath)
            # Remove .consumer extension to group related files
            base_key = basename.replace('.consumer', '')
            file_groups[base_key].append(filepath)
        
        for base_key, file_list in file_groups.items():
            # Parse all files in this group and merge their data
            merged_data_points = defaultdict(dict)  # time -> metric -> value
            scenario = None
            
            for filepath in file_list:
                file_scenario, data_points = self.parse_file(filepath)
                if not data_points:
                    continue
                    
                if scenario is None:
                    scenario = file_scenario
                
                # Merge data points by time
                for point in data_points:
                    time_key = point['time']
                    for metric in ['sent', 'confirmed', 'received', 'consumer_latency_median', 'confirm_latency_median']:
                        if point[metric] is not None:
                            merged_data_points[time_key][metric] = point[metric]
            
            if scenario and merged_data_points:
                # Convert merged data back to list format
                final_data_points = []
                for time_key in sorted(merged_data_points.keys()):
                    point = {'time': time_key}
                    point.update(merged_data_points[time_key])
                    # Fill missing metrics with None
                    for metric in ['sent', 'confirmed', 'received', 'consumer_latency_median', 'confirm_latency_median']:
                        if metric not in point:
                            point[metric] = None
                    final_data_points.append(point)
                
                # Create series name
                timestamp_part = base_key.split('-')[0:2]  # Get YYYYMMDD-HHMMSS part
                timestamp = '-'.join(timestamp_part)
                series_name = f"{scenario}\n({timestamp})"
                
                # Store merged data points for this series
                all_data['sent'][series_name] = final_data_points
                all_data['confirmed'][series_name] = final_data_points
                all_data['received'][series_name] = final_data_points
                all_data['consumer_latency_median'][series_name] = final_data_points
                all_data['confirm_latency_median'][series_name] = final_data_points
        
        # Generate plots for each metric
        metrics = ['sent', 'confirmed', 'received', 'consumer_latency_median', 'confirm_latency_median']
        generated_plots = []
        
        for metric in metrics:
            if metric in all_data and all_data[metric]:
                plot_path = self.create_plot(metric, all_data[metric], filter_name)
                generated_plots.append(plot_path)
        
        print(f"\nGenerated {len(generated_plots)} plots in {self.plots_dir}")
        return generated_plots


def main():
    parser = argparse.ArgumentParser(description='Parse RabbitMQ performance test results and generate plots')
    parser.add_argument('filter', help='Filter name to match files (e.g., "baseline")')
    parser.add_argument('-n', '--max-files', type=int, 
                       help='Maximum number of files to process (newest first)')
    parser.add_argument('-d', '--results-dir', default='results',
                       help='Directory containing result files (default: results)')
    
    args = parser.parse_args()
    
    # Initialize parser
    perf_parser = PerfTestParser(args.results_dir)
    
    # Process files and generate plots
    perf_parser.process_files(args.filter, args.max_files)


if __name__ == '__main__':
    main()