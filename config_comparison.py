import json
import pandas as pd
import re
from typing import Dict, List, Tuple, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    Load and parse JSON file with comprehensive error handling.
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        Dict[str, Any]: Parsed JSON content or empty dict if error occurs
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            logger.info(f"Successfully loaded JSON file: {file_path}")
            return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {file_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return {}

def parse_whitelist_manual(file_path: str) -> Dict[str, Any]:
    """
    Manually parse the whitelist file with its specific YAML-like format.
    Handles the custom format where sections are defined with colons and requirements are listed.
    
    Args:
        file_path (str): Path to the whitelist file
        
    Returns:
        Dict[str, Any]: Parsed whitelist data with must_include requirements for each section
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        parsed_data = {}
        current_section = None
        current_items = []
        
        for line in lines:
            line = line.rstrip('\r\n')
            
            # Check if this is a section header (ends with :)
            if line and not line.startswith(' ') and line.endswith(':'):
                # Save previous section if exists
                if current_section and current_items:
                    parsed_data[current_section] = {'must_include': current_items}
                
                # Start new section
                current_section = line[:-1]  # Remove the colon
                current_items = []
            
            # Check if this is a must_include line
            elif line.strip() == 'must_include:':
                continue  # Skip this line
            
            # Check if this is an item (starts with quote or is indented)
            elif line and (line.startswith('"') or line.startswith('  ')):
                item = line.strip()
                if item.startswith('"') and item.endswith('"'):
                    item = item[1:-1]  # Remove quotes
                elif item.startswith('- '):
                    item = item[2:]  # Remove list marker
                    if item.startswith('"') and item.endswith('"'):
                        item = item[1:-1]
                
                if item:
                    current_items.append(item)
        
        # Don't forget the last section
        if current_section and current_items:
            parsed_data[current_section] = {'must_include': current_items}
        
        logger.info(f"Successfully parsed whitelist file with {len(parsed_data)} sections")
        return parsed_data
        
    except Exception as e:
        logger.error(f"Error manually parsing whitelist file: {e}")
        return {}

def trim_whitespace(text: str) -> str:
    """
    Remove leading and trailing whitespace from text.
    
    Args:
        text (str): Input text to trim
        
    Returns:
        str: Trimmed text
    """
    return text.strip() if isinstance(text, str) else str(text).strip()

def extract_hostname_from_output(output_data: Dict[str, Any]) -> str:
    """
    Extract hostname from output data structure.
    Searches through various sections to find the device hostname.
    
    Args:
        output_data (Dict[str, Any]): Output JSON data
        
    Returns:
        str: Device hostname or 'Unknown-Device' if not found
    """
    try:
        # Try to find hostname in version or run_config
        if 'data' in output_data:
            data = output_data['data']
            
            # Check version section
            if 'version' in data and isinstance(data['version'], list):
                for line in data['version']:
                    if isinstance(line, str) and 'hostname' in line.lower():
                        match = re.search(r'hostname\s+(\S+)', line, re.IGNORECASE)
                        if match:
                            return match.group(1)
            
            # Check run_config section
            if 'run_config' in data and isinstance(data['run_config'], str):
                lines = data['run_config'].split('\n')
                for line in lines:
                    if 'hostname' in line.lower():
                        match = re.search(r'hostname\s+(\S+)', line, re.IGNORECASE)
                        if match:
                            return match.group(1)
        
        # Default fallback
        return "Unknown-Device"
        
    except Exception as e:
        logger.error(f"Error extracting hostname: {e}")
        return "Unknown-Device"

def parse_whitelist_section(section_data: Any) -> List[str]:
    """
    Parse whitelist section data into a list of required configurations.
    
    Args:
        section_data: Whitelist section data (dict with must_include key)
        
    Returns:
        List[str]: List of required configuration lines
    """
    try:
        config_lines = []
        
        if isinstance(section_data, dict):
            if 'must_include' in section_data:
                must_include = section_data['must_include']
                if isinstance(must_include, list):
                    config_lines.extend([trim_whitespace(line) for line in must_include if line])
                elif isinstance(must_include, str):
                    # Split by newlines and clean up
                    lines = must_include.split('\n')
                    for line in lines:
                        line = trim_whitespace(line)
                        if line and not line.startswith('#'):  # Skip comments
                            # Remove quotes if present
                            line = line.strip('"').strip("'")
                            if line:
                                config_lines.append(line)
        
        return [line for line in config_lines if line]  # Remove empty lines
        
    except Exception as e:
        logger.error(f"Error parsing whitelist section: {e}")
        return []

def parse_output_section(section_data: Any, section_name: str) -> List[str]:
    """
    Parse output section data into a list of current configurations.
    Handles different data formats including Ansible output format.
    
    Args:
        section_data: Output section data (can be dict, list, or string)
        section_name (str): Name of the section being parsed
        
    Returns:
        List[str]: List of current configuration lines
    """
    try:
        config_lines = []
        
        # Special handling for VTY section which is wrapped in Ansible output
        if section_name == 'vty' and isinstance(section_data, dict):
            if 'stdout_lines' in section_data:
                stdout_lines = section_data['stdout_lines']
                if isinstance(stdout_lines, list) and stdout_lines:
                    for line_group in stdout_lines:
                        if isinstance(line_group, list):
                            for line in line_group:
                                line = trim_whitespace(line)
                                if line and not line.startswith('!'):
                                    config_lines.append(line)
            elif 'stdout' in section_data:
                stdout = section_data['stdout']
                if isinstance(stdout, list) and stdout:
                    for item in stdout:
                        if isinstance(item, str):
                            lines = item.split('\n')
                            for line in lines:
                                line = trim_whitespace(line)
                                if line and not line.startswith('!'):
                                    config_lines.append(line)
        
        # Handle list format
        elif isinstance(section_data, list):
            for item in section_data:
                if isinstance(item, str):
                    line = trim_whitespace(item)
                    if line and not line.startswith('!') and line != '':
                        config_lines.append(line)
                elif isinstance(item, list):
                    # Handle nested lists (like ACL entries)
                    line = ' '.join([str(x) for x in item if x])
                    line = trim_whitespace(line)
                    if line and not line.startswith('!'):
                        config_lines.append(line)
        
        # Handle string format
        elif isinstance(section_data, str):
            lines = section_data.split('\n')
            for line in lines:
                line = trim_whitespace(line)
                if line and not line.startswith('!') and line != '':
                    config_lines.append(line)
        
        return config_lines
        
    except Exception as e:
        logger.error(f"Error parsing output section {section_name}: {e}")
        return []

def compare_configurations(output_config: List[str], whitelist_config: List[str], 
                         hostname: str, section_name: str) -> Tuple[List[str], List[str]]:
    """
    Compare output configuration with whitelist configuration.
    Identifies missing configurations and additional configurations.
    
    Args:
        output_config (List[str]): Current device configuration
        whitelist_config (List[str]): Required configuration from whitelist
        hostname (str): Device hostname
        section_name (str): Configuration section name
        
    Returns:
        Tuple[List[str], List[str]]: (missing_configs, additional_configs)
    """
    try:
        missing_configs = []
        additional_configs = []
        
        # Normalize configurations for comparison (trim and lowercase)
        output_normalized = [trim_whitespace(line.lower()) for line in output_config]
        whitelist_normalized = [trim_whitespace(line.lower()) for line in whitelist_config]
        
        # Find missing configurations
        for i, required_line in enumerate(whitelist_config):
            required_normalized = trim_whitespace(required_line.lower())
            
            # Skip placeholder lines (containing brackets)
            if '[' in required_normalized and ']' in required_normalized:
                continue
                
            if required_normalized not in output_normalized:
                missing_configs.append(f"missing config: {required_line}:{hostname}")
        
        # Find additional configurations (configurations in output but not in whitelist)
        for i, current_line in enumerate(output_config):
            current_normalized = trim_whitespace(current_line.lower())
            
            # Skip very common/basic lines that might be system generated
            skip_patterns = ['building configuration', 'current configuration', 'version', 'service timestamps']
            if any(pattern in current_normalized for pattern in skip_patterns):
                continue
                
            if current_normalized not in whitelist_normalized:
                # Check if this might be a dynamic/variable line matching a placeholder
                is_dynamic = False
                for whitelist_line in whitelist_normalized:
                    if '[' in whitelist_line and ']' in whitelist_line:
                        # Create pattern from whitelist line with placeholders
                        pattern = whitelist_line
                        pattern = pattern.replace('[x.x.x.x]', r'\d+\.\d+\.\d+\.\d+')
                        pattern = pattern.replace('[community string]', r'\S+')
                        pattern = pattern.replace('[username]', r'\S+')
                        pattern = pattern.replace('[building, site, country]', r'.+')
                        pattern = pattern.replace('[description]', r'.+')
                        
                        try:
                            if re.match(pattern, current_normalized):
                                is_dynamic = True
                                break
                        except:
                            continue  # Skip if regex pattern is invalid
                
                if not is_dynamic:
                    additional_configs.append(f"additional config: {current_line}:{hostname}")
        
        return missing_configs, additional_configs
        
    except Exception as e:
        logger.error(f"Error comparing configurations for {section_name}: {e}")
        return [], []

def process_device_comparison(output_data: Dict[str, Any], whitelist_data: Dict[str, Any]) -> Tuple[List[Dict], List[str]]:
    """
    Process comparison between device output and whitelist configurations.
    Maps whitelist sections to corresponding output sections and performs comparison.
    
    Args:
        output_data (Dict[str, Any]): Device output data
        whitelist_data (Dict[str, Any]): Whitelist configuration data
        
    Returns:
        Tuple[List[Dict], List[str]]: (summary_data, issues_list)
    """
    try:
        hostname = extract_hostname_from_output(output_data)
        logger.info(f"Processing device: {hostname}")
        
        summary_data = []
        issues_list = []
        
        # Get device data
        device_data = output_data.get('data', {})
        
        # Section mapping between whitelist names and actual output section names
        section_mapping = {
            'vty': 'vty',
            'snmp_Run': 'snmp',
            'snmp_run': 'snmp',
            'dhcp': 'dhcp',
            'tacacs': 'TACACS',
            'vlan': 'vlan',
            'logging': 'Log_server',
            'mtu': 'mtu',
            'vtyaccess_acl': 'vty_ACL',
            'snmp_ro_acl': 'snmp_ACL',
            'source_interface': 'run_config',
            'ntp': 'ntp',
            'interface_section': 'run_config',
            'version': 'version',
            'license': 'License_status',
            'clock_detail': 'clock',
        }
        
        # Process each section in whitelist
        for section_name, whitelist_section in whitelist_data.items():
            try:
                logger.info(f"Processing section: {section_name}")
                
                # Parse whitelist requirements
                required_configs = parse_whitelist_section(whitelist_section)
                
                if not required_configs:
                    logger.warning(f"No required configurations found for section: {section_name}")
                    continue
                
                # Find corresponding section in output data
                output_section = None
                mapped_section = section_mapping.get(section_name, section_name)
                
                if mapped_section in device_data:
                    output_section = device_data[mapped_section]
                else:
                    # Try case-insensitive match
                    for key in device_data.keys():
                        if key.lower() == mapped_section.lower():
                            output_section = device_data[key]
                            break
                
                # Parse current configurations
                current_configs = []
                if output_section is not None:
                    current_configs = parse_output_section(output_section, section_name)
                
                # Compare configurations
                missing_configs, additional_configs = compare_configurations(
                    current_configs, required_configs, hostname, section_name
                )
                
                # Determine status
                status = "OK" if not missing_configs and not additional_configs else "to_check"
                
                # Add to summary
                summary_data.append({
                    'section': section_name,
                    'hostname': hostname,
                    'status': status,
                    'missing_count': len(missing_configs),
                    'additional_count': len(additional_configs)
                })
                
                # Add issues to list
                issues_list.extend(missing_configs)
                issues_list.extend(additional_configs)
                
                logger.info(f"Section {section_name}: {len(missing_configs)} missing, {len(additional_configs)} additional")
                
            except Exception as e:
                logger.error(f"Error processing section {section_name}: {e}")
                summary_data.append({
                    'section': section_name,
                    'hostname': hostname,
                    'status': 'error',
                    'missing_count': 0,
                    'additional_count': 0
                })
        
        return summary_data, issues_list
        
    except Exception as e:
        logger.error(f"Error processing device comparison: {e}")
        return [], []

def create_excel_report(summary_data: List[Dict], issues_list: List[str], output_file: str = 'config_comparison.xlsx') -> bool:
    """
    Create Excel report with three sheets: Summary, Issues_to_Check, and Statistics.
    
    Args:
        summary_data (List[Dict]): Summary data for first sheet
        issues_list (List[str]): List of issues for second sheet
        output_file (str): Output Excel file path
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Summary
            if summary_data:
                # Get unique hostname
                hostname = summary_data[0]['hostname'] if summary_data else 'Unknown'
                
                # Create summary dataframe
                summary_df_data = {
                    'SR.No.': [1],
                    'hostname of the devices': [hostname]
                }
                
                # Add section columns
                for item in summary_data:
                    section_name = item['section'].upper()
                    summary_df_data[section_name] = [item['status']]
                
                summary_df = pd.DataFrame(summary_df_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                logger.info(f"Created Summary sheet with {len(summary_data)} sections")
            
            # Sheet 2: Issues to Check
            if issues_list:
                issues_df = pd.DataFrame({'to_check': issues_list})
                issues_df.to_excel(writer, sheet_name='Issues_to_Check', index=False)
                logger.info(f"Created Issues_to_Check sheet with {len(issues_list)} issues")
            else:
                # Create empty sheet with header
                issues_df = pd.DataFrame({'to_check': []})
                issues_df.to_excel(writer, sheet_name='Issues_to_Check', index=False)
            
            # Sheet 3: Statistics
            if summary_data:
                hostname = summary_data[0]['hostname']
                total_sections = len(summary_data)
                total_issues = len(issues_list)
                missing_configs = len([issue for issue in issues_list if 'missing config:' in issue])
                additional_configs = len([issue for issue in issues_list if 'additional config:' in issue])
                
                # Calculate compliance score
                compliance_score = max(0, ((total_sections - len([s for s in summary_data if s['status'] == 'to_check'])) / total_sections * 100)) if total_sections > 0 else 100
                
                stats_data = {
                    'Metric': [
                        'Device Hostname',
                        'Total Sections Checked',
                        'Total Issues Found',
                        'Missing Configurations',
                        'Additional Configurations',
                        'Compliance Score'
                    ],
                    'Count': [
                        hostname,
                        total_sections,
                        total_issues,
                        missing_configs,
                        additional_configs,
                        f"{compliance_score:.0f}%"
                    ]
                }
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Statistics', index=False)
                logger.info("Created Statistics sheet")
        
        logger.info(f"Excel report created successfully: {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating Excel report: {e}")
        return False

def main():
    """
    Main function to orchestrate the configuration comparison process.
    Update the file paths below to point to your actual JSON files.
    """
    try:
        # File paths - UPDATE THESE PATHS TO YOUR ACTUAL FILES
        output_file_path = "C:\\Users\\Simco\\Desktop\\Python-test-scripts\\comparision script\\New_output.json"      # Path to your device output JSON file
        whitelist_file_path = "Whitelist_file.json"  # Path to your whitelist file
        excel_output_path = "config_comparison.xlsx"  # Output Excel file name
        
        logger.info("Starting configuration comparison process...")
        
        # Load JSON files
        logger.info("Loading JSON files...")
        output_data = load_json_file(output_file_path)
        whitelist_data = parse_whitelist_manual(whitelist_file_path)
        
        if not output_data:
            logger.error("Failed to load output data file")
            return False
            
        if not whitelist_data:
            logger.error("Failed to load whitelist data file")
            return False
        
        # Process comparison
        logger.info("Processing device comparison...")
        summary_data, issues_list = process_device_comparison(output_data, whitelist_data)
        
        if not summary_data:
            logger.warning("No summary data generated")
            return False
        
        # Create Excel report
        logger.info("Creating Excel report...")
        success = create_excel_report(summary_data, issues_list, excel_output_path)
        
        if success:
            logger.info("Configuration comparison completed successfully!")
            print(f"Report generated: {excel_output_path}")
            print(f"Total sections processed: {len(summary_data)}")
            print(f"Total issues found: {len(issues_list)}")
            
            # Display summary
            print("\nSummary by section:")
            for item in summary_data:
                print(f"  {item['section']}: {item['status']} ({item['missing_count']} missing, {item['additional_count']} additional)")
        else:
            logger.error("Failed to create Excel report")
            
        return success
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        return False

if __name__ == "__main__":
    main()