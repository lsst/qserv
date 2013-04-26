
# Pickup one line each 1000 lines
/./ { if ((NR % 1000) == 0) print ; }
