import os

def ensure_redis_structure(config_path: str):
    """Ensure the required Redis directories and files exist based on the config file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of this script
    config_path = os.path.join(script_dir, config_path)  # Full path to the config file

    with open(config_path, 'r') as conf_file:
        for line in conf_file:
            # Strip comments and whitespace
            line = line.split('#')[0].strip()
            
            if line.startswith('logfile'):
                # Use only the filename, resolve within script_dir
                logfile_path = os.path.join(script_dir, os.path.basename(line.split()[1]))
                if not os.path.exists(logfile_path):
                    open(logfile_path, 'w').close()
                    print(f"Created log file: {logfile_path}")
                    
            elif line.startswith('dir'):
                # Use only the directory name, resolve within script_dir
                data_dir = os.path.join(script_dir, os.path.basename(line.split()[1]))
                if not os.path.exists(data_dir):
                    os.makedirs(data_dir)
                    print(f"Created data directory: {data_dir}")

# Ensure directories and files before running Redis
if __name__ == "__main__":
    ensure_redis_structure('redis.conf')  # Config file is in the same folder
    print("All necessary directories and files have been ensured.")
