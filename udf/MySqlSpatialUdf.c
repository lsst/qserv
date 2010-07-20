/** @file
  * @brief MySQL spatial UDFs for qserv.
  *
  * Provided are methods for
  * @li computing the angular separation between two points on the unit sphere
  * @li testing whether points belong to spherical boxes, circles, ellipses and
  *     convex polygons.
  *
  * @author Serge Monkewitz
  */
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>

#include "mysql/mysql.h"

#ifdef __cplusplus
extern "C" {
#endif

static double const QSERV_DEG_PER_RAD = 180.0 / M_PI;
static double const QSERV_RAD_PER_DEG = M_PI / 180.0;
static double const QSERV_ARCSEC_PER_DEG = 3600.0;


/* -- Angular separation -------- */

/** Returns D^2/4, where D is the euclidian distance between the two
    input points on the unit sphere. */
static double _qserv_dist(double ra1, double dec1, double ra2, double dec2) {
    double x, y, z, dist;
    x = sin((ra1 - ra2) * QSERV_RAD_PER_DEG * 0.5);
    x *= x;
    y = sin((dec1 - dec2) * QSERV_RAD_PER_DEG * 0.5);
    y *= y;
    z = cos((dec1 + dec2) * QSERV_RAD_PER_DEG * 0.5);
    z *= z;
    dist = x * (z - y) + y;
    return dist < 0.0 ? 0.0 : (dist > 1.0 ? 1.0 : dist);
}

static double _qserv_angSep(double ra1, double dec1, double ra2, double dec2) {
    double dist = _qserv_dist(ra1, dec1, ra2, dec2);
    return 2.0 * QSERV_DEG_PER_RAD * asin(sqrt(dist));
}

my_bool qserv_angSep_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    int i;
    my_bool maybe_null = 0, const_item = 1;
    if (args->arg_count != 4) {
        strcpy(message, "qserv_angSep() expects 4 arguments");
        return 1;
    }
    for (i = 0; i < 4; ++i) {
        args->arg_type[i] = REAL_RESULT;
        if (args->maybe_null[i] == 1) {
            maybe_null = 1;
        }
        if (args->args[i] == 0) {
            const_item = 0;
        }
    }
    initid->maybe_null = maybe_null;
    initid->const_item = const_item;
    initid->decimals = 31;
    return 0;
}

/** Returns the angular separation in degrees between two spherical
  * coordinate pairs (ra1, dec1) and (ra2, dec2).
  *
  * Consumes 4 arguments ra1, dec1, ra2 and dec2 all of type REAL:
  * @li ra1:  right ascension of the first position (deg)
  * @li dec1: declination of the first position (deg)
  * @li ra2:  right ascension of the second position (deg)
  * @li dec2: declination of the second position (deg)
  *
  * Also:
  * @li If any parameter is NULL, NULL is returned.
  * @li If dec1 or dec2 lies outside of [-90, 90], this is an error
  *     and NULL is returned.
  */
double qserv_angSep(UDF_INIT *initid,
                    UDF_ARGS *args,
                    char *is_null,
                    char *error)
{
    int i;
    double dec1, dec2;

    /* If any input is null, the result is null. */
    for (i = 0; i < 4; ++i) {
        if (args->args[i] == 0) {
            *is_null = 1;
            return 0.0;
        }
    }
    /* Check that dec lies in range. */
    dec1 = *(double *) args->args[1];
    dec2 = *(double *) args->args[3];
    if (dec1 < -90.0 || dec1 > 90.0 || dec2 < -90.0 || dec2 > 90.0) {
        *is_null = 1;
        return 0.0;
    }
    return _qserv_angSep(*(double *) args->args[0], dec1,
                         *(double *) args->args[2], dec2);
}


/* -- Point in spherical box test -------- */

/** Range reduces the given angle to lie in the range [0.0, 360.0). */
static double _qserv_reduceRa(double theta) {
    if (theta < 0.0 || theta >= 360.0) {
        theta = fmod(theta, 360.0);
        if (theta < 0.0) {
            theta += 360.0;
        }
    }
    return theta;
}

my_bool qserv_ptInSphBox_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    int i;
    my_bool const_item = 1;
    if (args->arg_count != 6) {
        strcpy(message, "qserv_ptInSphBox() expects 6 arguments");
        return 1;
    }
    for (i = 0; i < 6; ++i) {
        args->arg_type[i] = REAL_RESULT;
        if (args->args[i] == 0) {
            const_item = 0;
        }
    }
    initid->maybe_null = 1;
    initid->const_item = const_item;
    return 0;
}

/** Returns 1 if the given spherical longitude/latitude box contains
  * the given position, and 0 otherwise.
  *
  * Consumes 6 arguments ra, dec, ra_min, dec_min, ra_max and dec_max, in
  * that order, all of type REAL and in units of degrees. (ra, dec) is the
  * position to test - the remaining parameters specify the spherical box.
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec, dec_min or dec_max lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If dec_min is greater than dec_max, the spherical box is empty
  *     and 0 is returned.
  * @li If both ra_min and ra_max lie in the range [0, 360], then ra_max
  *     can be less than ra_min. For example, a box with ra_min = 350
  *     and ra_max = 10 includes points with right ascensions in the ranges
  *     [350, 360) and [0, 10].
  * @li If either ra_min or ra_max lies outside of [0, 360], then ra_min
  *     must be <= ra_max (otherwise, NULL is returned), though the values
  *     can be arbitrary. If the two are separated by 360 degrees or more,
  *     then the box spans [0, 360). Otherwise, both values are range reduced.
  *     For example, a spherical box with ra_min = 350 and ra_max = 370
  *     includes points with right ascensions in the rnages [350, 360) and
  *     [0, 10].
  */
long long qserv_ptInSphBox(UDF_INIT *initid,
                           UDF_ARGS *args,
                           char *is_null,
                           char *error)
{
    double ra, dec, ra_min, dec_min, ra_max, dec_max;
    int i;

    /* If any input is null, the result is 0. */
    for (i = 0; i < 6; ++i) {
        if (args->args[i] == 0) {
            return 0;
        }
    }
    dec     = *(double *) args->args[1];
    ra_min  = *(double *) args->args[2];
    dec_min = *(double *) args->args[3];
    ra_max  = *(double *) args->args[4];
    dec_max = *(double *) args->args[5];
    /* Check arguments. */
    if (dec < -90.0 || dec_min < -90.0 || dec_max < -90.0 ||
        dec > 90.0 || dec_min > 90.0 || dec_max > 90.0) {
        *is_null= 1;
        return 0;
    }
    if (ra_max < ra_min && (ra_max < 0.0 || ra_min > 360.0)) {
        *is_null= 1;
        return 0;
    }
    if (dec_min > dec_max || dec < dec_min || dec > dec_max) {
        return 0;
    }
    /* Range-reduce longitude angles */
    ra = _qserv_reduceRa(*(double *) args->args[0]);
    if (ra_max - ra_min >= 360.0) {
        ra_min = 0.0;
        ra_max = 360.0;
    } else {
        ra_min = _qserv_reduceRa(ra_min);
        ra_max = _qserv_reduceRa(ra_max);
    }
    if (ra_min <= ra_max) {
        return ra >= ra_min && ra <= ra_max;
    } else {
        return ra >= ra_min || ra <= ra_max;
    }
}


/* -- Point in spherical circle test -------- */

typedef struct {
    double dist;
    int valid;
} _qserv_dist_t;

my_bool qserv_ptInSphCircle_init(UDF_INIT *initid,
                                 UDF_ARGS *args,
                                 char *message)
{
    int i;
    my_bool const_item = 1;
    if (args->arg_count != 5) {
        strcpy(message, "qserv_ptInSphCircle() expects 5 arguments");
        return 1;
    }
    for (i = 0; i < 5; ++i) {
        args->arg_type[i] = REAL_RESULT;
        if (args->args[i] == 0) {
            const_item = 0;
        }
    }
    initid->maybe_null = 1;
    initid->const_item = const_item;
    initid->ptr = 0;
    /* For constant radius circles, cache (sin(radius/2))^2 across calls. */
    if (args->args[4] != 0) {
        initid->ptr = (char *) calloc(1, sizeof(_qserv_dist_t));
    }
    return 0;
}

void qserv_ptInSphCircle_deinit(UDF_INIT *initid) {
    free(initid->ptr);
}

/** Returns 1 if the given circle on the unit sphere contains
  * the specified position and 0 otherwise.
  *
  * Consumes 5 arguments, all of type REAL:
  * @li ra:       right ascension of position to test (deg)
  * @li dec:      declination of position to test (deg)
  * @li ra_cen:   right ascension of circle center (deg)
  * @li dec_cen:  declination of circle center (deg)
  * @li radius:   radius (opening angle) of circle (deg)
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec or dec_cen lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If radius is negative or greater than 180, this is
  *     an error and NULL is returned.
  */
long long qserv_ptInSphCircle(UDF_INIT *initid,
                              UDF_ARGS *args,
                              char *is_null,
                              char *error)
{
    int i;
    double r, d, dec, decCen, deltaDec;

    /* If any input is null, the result is 0. */
    for (i = 0; i < 5; ++i) {
        if (args->args[i] == 0) {
            return 0;
        }
    }
    dec = *(double *) args->args[1];
    decCen = *(double *) args->args[3];
    r = *(double *) args->args[4];
    /* Check arguments */
    if (dec < -90.0 || dec > 90.0 || decCen < -90.0 || decCen > 90.0) {
        *is_null = 1;
        return 0;
    }
    if (r < 0.0 || r > 180.0) {
        *is_null = 1;
        return 0;
    }
    deltaDec = fabs(dec - decCen);
    /* Fail-fast if declination delta exceeds the radius. */
    if (deltaDec > r) {
        return 0;
    }
    if (initid->ptr == 0) {
        d = _qserv_angSep(*(double *) args->args[0], dec,
                          *(double *) args->args[2], decCen);
    } else {
        /* Avoids an asin and sqrt for constant radii */
        _qserv_dist_t *cache = (_qserv_dist_t *) initid->ptr;
        if (cache->valid == 0) {
            double dist;
            dist = sin(r * 0.5 * QSERV_RAD_PER_DEG);
            dist *= dist;
            cache->dist = dist;
            cache->valid = 1;
        }
        r = cache->dist;
        d = _qserv_dist(*(double *) args->args[0], dec,
                        *(double *) args->args[2], decCen);
    }
    return d <= r;
}


/* -- Point in spherical ellipse test -------- */

typedef struct {
    double sinRa;     /**< sine of ellipse center longitude angle */
    double cosRa;     /**< cosine of ellipse center longitude angle */
    double sinDec;    /**< sine of ellipse center latitude angle */
    double cosDec;    /**< cosine of ellipse center latitude angle */
    double sinAng;    /**< sine of ellipse position angle */
    double cosAng;    /**< cosine of ellipse position angle */
    double invMinor2; /**< 1/(m*m); m = semi-minor axis length (rad) */
    double invMajor2; /**< 1/(M*M); M = semi-major axis length (rad) */
    int valid;
} _qserv_sphEllipse_t;

my_bool qserv_ptInSphEllipse_init(UDF_INIT *initid,
                                  UDF_ARGS *args,
                                  char *message)
{
    int i;
    my_bool const_item = 1, const_ellipse = 1;
    if (args->arg_count != 7) {
        strcpy(message, "qserv_ptInSphEllipse() expects 7 arguments");
        return 1;
    }
    for (i = 0; i < 7; ++i) {
        args->arg_type[i] = REAL_RESULT;
        if (args->args[i] == 0) {
            const_item = 0;
            if (i >= 2) {
                const_ellipse = 0;
            }
        }
    }
    initid->maybe_null = 1;
    initid->const_item = const_item;
    initid->ptr = 0;
    /* If ellipse parameters are constant, allocate derived quantity cache. */
    if (const_ellipse) {
        initid->ptr = (char *) calloc(1, sizeof(_qserv_sphEllipse_t));
    }
    return 0;
}

void qserv_ptInSphEllipse_deinit(UDF_INIT *initid) {
    free(initid->ptr);
}

/** Returns 1 if the given ellipse on the unit sphere contains
  * the specified position and 0 otherwise.
  *
  * Consumes 7 arguments, all of type REAL:
  * @li ra:       right ascension of position to test (deg)
  * @li dec:      declination of position to test (deg)
  * @li ra_cen:   right ascension of ellipse center (deg)
  * @li dec_cen:  declination of ellipse center (deg)
  * @li smaa:     semi-major axis length (arcsec)
  * @li smia:     semi-minor axis length (arcsec)
  * @li ang:      ellipse position angle (deg)
  *
  * Note that:
  * @li If any parameter is NULL, the return value is 0.
  * @li If dec or dec_cen lies outside of [-90, 90],
  *     this is an error and NULL is returned.
  * @li If smia is negative or greater than smaa, this is
  *     an error and NULL is returned.
  * @li If smaa is greater than 36000 arcsec (10 deg), this
  *     is an error and NULL is returned.
  */
long long qserv_ptInSphEllipse(UDF_INIT *initid,
                               UDF_ARGS *args,
                               char *is_null,
                               char *error)
{
    _qserv_sphEllipse_t ellipse;
    double m, M, ra, dec, decCen, x, y, z, w, xne, yne;
    _qserv_sphEllipse_t *ep;
    int i;

    /* If any input is null, the result is 0. */
    for (i = 0; i < 7; ++i) {
        if (args->args[i] == 0) {
            return 0;
        }
    }
    /* Check arguments */
    dec = *(double *) args->args[1];
    decCen = *(double *) args->args[3];
    if (dec < -90.0 || dec > 90.0 || decCen < -90.0 || decCen > 90.0) {
        *is_null = 1;
        return 0;
    }
    /* Semi-minor axis length m and semi-major axis length M must satisfy
       0 <= m <= M <= 10 deg */
    m = *(double *) args->args[5];
    M = *(double *) args->args[4];
    if (m < 0.0 || m > M || M > 10.0 * QSERV_ARCSEC_PER_DEG) {
        *is_null = 1;
        return 0;
    }
    ellipse.valid = 0;
    ep = (initid->ptr != 0) ? (_qserv_sphEllipse_t *) initid->ptr : &ellipse;
    if (ep->valid == 0) {
        double raCen = *(double *) args->args[2] * QSERV_RAD_PER_DEG;
        double ang = *(double *) args->args[6] * QSERV_RAD_PER_DEG;
        decCen *= QSERV_RAD_PER_DEG;
#ifdef QSERV_HAVE_SINCOS
        sincos(raCen, &ep->sinRa, &ep->cosRa);
        sincos(decCen, &ep->sinDec, &ep->cosDec);
        sincos(ang, &ep->sinAng, &ep->cosAng);
#else
        ep->sinRa = sin(raCen);
        ep->cosRa = cos(raCen);
        ep->sinDec = sin(decCen);
        ep->cosDec = cos(decCen);
        ep->sinAng = sin(ang);
        ep->cosAng = cos(ang);
#endif
        m = m * QSERV_RAD_PER_DEG / QSERV_ARCSEC_PER_DEG;
        M = M * QSERV_RAD_PER_DEG / QSERV_ARCSEC_PER_DEG;
        ep->invMinor2 = 1.0 / (m * m);
        ep->invMajor2 = 1.0 / (M * M);
        ep->valid = 1;
    }
    /* Transform input position from spherical coordinates
       to a unit cartesian vector. */
    ra = *(double *) args->args[0] * QSERV_RAD_PER_DEG;
    dec *= QSERV_RAD_PER_DEG;
#ifdef QSERV_HAVE_SINCOS
    sincos(ra, &y, &x);
    sincos(dec, &z, &w);
#else
    x = cos(ra);
    y = sin(ra);
    z = sin(dec);
    w = cos(dec);
#endif
    x *= w;
    y *= w;
    /* get coords of input point in (N,E) basis at ellipse center */
    xne = ep->cosDec * z - ep->sinDec * (ep->sinRa * y + ep->cosRa * x);
    yne = ep->cosRa * y - ep->sinRa * x;
    /* rotate by negated position angle */
    x = ep->sinAng * yne + ep->cosAng * xne;
    y = ep->cosAng * yne - ep->sinAng * xne;
    /* perform standard 2D axis-aligned point-in-ellipse test */
    return (x * x * ep->invMajor2 + y * y * ep->invMinor2 <= 1.0);
}


/* -- Point in spherical convex polygon test -------- */

typedef struct {
    double *edges;
    int nedges;
} _qserv_sphPoly_t;

static void _qserv_computeEdges(_qserv_sphPoly_t *poly,
                                double *verts,
                                unsigned long nv)
{
    double x, y, z, w, xp, yp, zp, xl, yl, zl;
    unsigned long i;
    /* Transform last vertex to a unit 3 vector */
#ifdef QSERV_HAVE_SINCOS
    sincos(verts[nv*2 - 2], &yl, &xl);
    sincos(verts[nv*2 - 1], &zl, &w);
#else
    xl = cos(verts[nv*2 - 2]);
    yl = sin(verts[nv*2 - 2]);
    zl = sin(verts[nv*2 - 1]);
    w = cos(verts[nv*2 - 1]);
#endif
    xl *= w;
    yl *= w;
    xp = xl; yp = yl; zp = zl;
    for (i = 0; i < nv - 1; ++i) {
        /* Transform current vertex to a unit 3 vector */
#ifdef QSERV_HAVE_SINCOS
        sincos(verts[i*2], &y, &x);
        sincos(verts[i*2 + 1], &z, &w);
#else
        x = cos(verts[i*2]);
        y = sin(verts[i*2]);
        z = sin(verts[i*2 + 1]);
        w = cos(verts[i*2 + 1]);
#endif
        x *= w;
        y *= w;
        /* Edge plane equation is the cross product of the 2 vertices */
        poly->edges[i*3] = yp * z - zp * y;
        poly->edges[i*3 + 1] = zp * x - xp * z;
        poly->edges[i*3 + 2] = xp * y - yp * x;
        xp = x; yp = y; zp = z;
    }
    /* Compute last edge plane equation */
    poly->edges[i*3] = yp * zl - zp * yl;
    poly->edges[i*3 + 1] = zp * xl - xp * zl;
    poly->edges[i*3 + 2] = xp * yl - yp * xl;
    poly->nedges = nv;
}

static int _qserv_parseSphPoly(_qserv_sphPoly_t *poly,
                               char const *spec,
                               unsigned long len)
{
    char buffer[256];
    double verts[16*2];
    double c;
    /* Use stack allocated buffers where possible. */
    double *v = verts;
    char *s = buffer, *vs = 0, *ep = 0;
    int n = 0, cap = 16*2;
    int ret = 1; /* return value, 1 means an error occurred */

    if (len > 255) {
        s = (char *) calloc(len + 1, 1);
        if (s == 0) {
            return 1;
        }
    }
    /* MySQL doesn't guarantee that string arguments are null terminated. */
    strncpy(s, spec, len);
    s[len] = '\0';
    /* eat leading whitespace */
    for (vs = s; vs < s + len && isspace(*vs); ++vs) { }
    while (vs < s + len) {
        if (n == cap) {
            /* more space for vertices needed */
            double *nv = (double *) malloc(cap * 2 * sizeof(double));
            if (nv == 0) {
                goto cleanup;
            }
            cap += cap;
            memcpy(nv, v, n * sizeof(double));
            if (v != verts) {
                free(v);
            }
            v = nv;
        }
        /* parse one coordinate */
        c = strtod(vs, &ep);
        if (ep == vs) {
            goto cleanup;
        }
        if ((n & 1) == 1 && (c < -90.0 || c > 90.0)) {
            goto cleanup;
        }
        v[n++] = c *  QSERV_RAD_PER_DEG;
        vs = ep;
        /* eat trailing whitespace */
        for (; vs < s + len && isspace(*vs); ++vs) { }
    }
    if (n >= 6 && (n & 1) == 0) {
        /* Have at least 3 coordinate pairs. */
        n /= 2;
        poly->edges = malloc(3 * n * sizeof(double));
        if (poly->edges != 0) {
            _qserv_computeEdges(poly, v, n);
            ret = 0; /* success! */
        }
    }
cleanup:
    /* cleanup allocated memory */
    if (s != buffer) {
        free(s);
    }
    if (v != verts) {
        free(v);
    }
    return ret;
}

static void _qserv_freeSphPoly(_qserv_sphPoly_t *poly) {
    if (poly != 0) {
        free(poly->edges);
        poly->edges = 0;
    }
}
 
my_bool qserv_ptInSphPoly_init(UDF_INIT *initid,
                               UDF_ARGS *args,
                               char *message)
{
    int i;
    my_bool const_item = 1, const_poly = 1;
    if (args->arg_count != 3) {
        strcpy(message, "qserv_ptInSphPoly() expects 3 arguments");
        return 1;
    }
    if (args->arg_type[2] != STRING_RESULT) {
        strcpy(message, "qserv_ptInSphPoly() expects polygon "
               "specification to be a string");
        return 1;
    }
    const_item = args->args[2] != 0;
    const_poly = args->args[2] != 0;
    for (i = 0; i < 2; ++i) {
        args->arg_type[i] = REAL_RESULT;
        if (args->args[i] == 0) {
            const_item = 0;
        }
    }
    initid->maybe_null = 1;
    initid->const_item = const_item;
    initid->ptr = 0;
    /* If polygon spec is constant, parse and cache it. */
    if (const_poly) {
        _qserv_sphPoly_t *poly = calloc(1, sizeof(_qserv_sphPoly_t));
        if (poly == 0) {
            strcpy(message, "qserv_ptInSphPoly(): failed to allocate memory "
                   "for polygon");
            return 1;
        }
        if (_qserv_parseSphPoly(poly, args->args[2], args->lengths[2]) != 0) {
            strcpy(message, "qserv_ptInSphPoly(): failed to parse spherical "
                   "convex polygon spec");
            free(poly);
            return 1;
        }
        initid->ptr = (char *) poly;
    }
    return 0;
}

void qserv_ptInSphPoly_deinit(UDF_INIT *initid) {
    _qserv_freeSphPoly((_qserv_sphPoly_t *) initid->ptr);
    free(initid->ptr);
}

/** Returns 1 if the given spherical convex polygon contains
  * the specified position and 0 otherwise.
  *
  * Consumes 3 arguments ra, dec and poly. The ra and dec parameters
  * must be convertible to a REAL, and poly must be a STRING.
  *
  * @li ra:    right ascension of position to test (deg)
  * @li dec:   declination of position to test (deg)
  * @li poly:  polygon specification
  *
  * Note that:
  * @li If any input parameter is NULL, 0 is returned.
  * @li If dec is outside of [-90,90], this is an error and NULL is returned.
  * @li If the polygon spec is invalid or cannot be parsed (e.g. because the
  *     the server has run out of memory), this is an error and the return
  *     value is NULL.
  *
  * A polygon specification consists of a space separated list of vertex
  * coordinate pairs: "ra_0 dec_0 ra_1 dec_1 .... ra_n dec_n". There must
  * be at least 3 coordinate pairs and declinations must lie in the range
  * [-90,90]. Also, if the following conditions are not satisfied, then the
  * return value of the function is undefined:
  *
  * @li vertices are hemispherical
  * @li vertices form a convex polygon when connected with great circle edges
  * @li vertices lie in counter-clockwise order when viewed from a position
  * @li outside the unit sphere and inside the half-space containing them.
  */
long long qserv_ptInSphPoly(UDF_INIT *initid,
                            UDF_ARGS *args,
                            char *is_null,
                            char *error)
{
    _qserv_sphPoly_t poly;
    double ra, dec, x, y, z, w;
    _qserv_sphPoly_t *pp;
    int i, ret = 0;

    /* If any input is null, the result is 0. */
    for (i = 0; i < 3; ++i) {
        if (args->args[i] == 0) {
            return 0;
        }
    }
    /* Check that dec is in range */
    dec = *(double *) args->args[1];
    if (dec < -90.0 || dec > 90.0) {
        *is_null = 1;
        return 0;
    }
    ra = *(double *) args->args[0] * QSERV_RAD_PER_DEG;
    dec *= QSERV_RAD_PER_DEG;
    /* Parse polygon spec if it isn't constant */
    if (initid->ptr != 0) {
        pp = (_qserv_sphPoly_t *) initid->ptr;
    } else {
        pp = &poly;
        if (_qserv_parseSphPoly(pp, args->args[2], args->lengths[2]) != 0) {
            *is_null = 1;
            return 0;
        }
    }
    /* Transform input position from spherical coordinates
       to a unit cartesian vector. */
#ifdef QSERV_HAVE_SINCOS
    sincos(ra, &y, &x);
    sincos(dec, &z, &w);
#else
    x = cos(ra);
    y = sin(ra);
    z = sin(dec);
    w = cos(dec);
#endif
    x *= w;
    y *= w;
    ret = 1;
    for (i = 0; i < pp->nedges; ++i) {
        /* compute dot product of edge plane normal and input position */
        double dp = x * pp->edges[i*3 + 0] +
                    y * pp->edges[i*3 + 1] +
                    z * pp->edges[i*3 + 2];
        if (dp < 0.0) {
            ret = 0;
            break;
        }
    }
    if (initid->ptr == 0) {
        _qserv_freeSphPoly(pp);
    }
    return ret;
}

#ifdef __cplusplus
} /* extern "C" */
#endif

