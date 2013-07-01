
BEGIN {
    while ((getline ID < "ObjectId.txt") > 0) 
        objectid[ID]=1;
}

/./ { if ($1 in objectid) print ; }

