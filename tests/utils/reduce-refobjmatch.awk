
BEGIN {
    while ((getline ID < "ObjectId.txt") > 0) 
        objectid[ID]=1;
}

/./ { if ($2 in objectid) print ; }

