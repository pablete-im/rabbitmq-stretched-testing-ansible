#!/bin/bash
# Installation script for performance test parser dependencies

echo "Installing Python dependencies for performance test parser..."

# Try different installation methods
if command -v apt &> /dev/null; then
    echo "Using apt to install system packages..."
    sudo apt update
    sudo apt install -y python3-matplotlib python3-numpy
elif command -v yum &> /dev/null; then
    echo "Using yum to install system packages..."
    sudo yum install -y python3-matplotlib python3-numpy
elif command -v dnf &> /dev/null; then
    echo "Using dnf to install system packages..."
    sudo dnf install -y python3-matplotlib python3-numpy
else
    echo "System package manager not found. Trying pip..."
    
    # Check if we can use pip
    if python3 -m pip --version &> /dev/null; then
        echo "Creating virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
        pip install matplotlib numpy
        echo "Dependencies installed in virtual environment."
        echo "To use the plotter, activate the environment first:"
        echo "  source plotter/venv/bin/activate"
        echo "  python3 plotter/parse_and_plot.py baseline"
    else
        echo "pip not available. Please install matplotlib and numpy manually."
        echo "On Debian/Ubuntu: sudo apt install python3-matplotlib python3-numpy"
        echo "On RHEL/CentOS: sudo yum install python3-matplotlib python3-numpy"
        echo "On Fedora: sudo dnf install python3-matplotlib python3-numpy"
    fi
fi

echo "Installation complete!"
echo ""
echo "You can now use:"
echo "  python3 plotter/parse_and_plot.py baseline      # Generate PNG plots (requires matplotlib)"
echo "  python3 plotter/parse_to_csv.py baseline        # Generate CSV files (no dependencies)"