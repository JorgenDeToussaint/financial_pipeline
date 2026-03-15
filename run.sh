#!/bin/bash
echo ""
echo "🌊 Financial Lakehouse Pipeline"
echo "================================"
echo "1) Run pipeline"
echo "2) Exit"
echo ""
read -p "Select: " choice

case $choice in
    1) python main.py ;;
    2) echo "Bye." ; exit 0 ;;
    *) echo "Invalid option" ; exit 1 ;;
esac