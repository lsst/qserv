
BEGIN {
    while ((getline ID < "SourceId.txt") > 0) 
        sourceid[ID]=1;
}

/./ { if ($2 in sourceid) print ; }

