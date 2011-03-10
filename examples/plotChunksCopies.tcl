#!/usr/bin/env wish

# A script that plots chunk boundaries and copy boundaries on a sphere.
# Hacked from K-T Lim's code to support plotting chunk and copy boundaries.

# file can be generated as the output of:
# makeChunk.py .... --print-plottable --explain ...
set plottableFile "/afs/slac/u/sf/danielw/current.plottable"

# Sphere radius
set r 550

# Viewing angle
set phi0 50
set theta0 90

set pi 3.14159265358979323846264

# Set up canvas to draw on
canvas .win -width [expr {$r * 2 + 20}] -height [expr {$r * 2 + 20}]
pack .win

# Draw the sphere outline and fill (would be nice to do a gradient...)
#.win create oval 20 20 [expr {$r * 2 + 19}] [expr {$r * 2 + 19}] -fill #c0e0ff
.win create oval 20 20 [expr {$r * 2 + 19}] [expr {$r * 2 + 19}] -fill #ffffff

# Degrees to radians converter
proc dtor {deg} {
	global pi
	return [expr {$deg * $pi / 180.0}]
}

# Radians to degrees converter
proc rtod {rad} {
	global pi
	return [expr {$rad * 180.0 / $pi}]
}

# Transform from spherical coordinates to projected canvas coordinates
proc transform {phi theta} {
	global r
	global phi0
	global theta0

	set px [expr {$r * cos([dtor $phi]) * sin([dtor $theta] - [dtor $theta0]) + $r}]
	set py [expr {$r -$r * (
			cos([dtor $phi0]) * sin([dtor $phi]) - \
			sin([dtor $phi0])*cos([dtor $phi])*cos([dtor $theta] - [dtor $theta0]) \
			)}]
	# This determines whether the point is in front of or behind the
	# projected plane
	set cosc [expr {sin([dtor $phi0]) * sin([dtor $phi]) + \
			cos([dtor $phi0])*cos([dtor $phi])*cos([dtor $theta] - [dtor $theta0])}]
	if {$cosc < 0} {
		# Hidden point - do not draw
		return {}
	} else {
		return [list [expr {$px + 20}] [expr {$py + 20}]]
	}
}


# Draw a longitude segment
proc drawLonLine {theta phi0 phi1 color} {
    set coordList {}
    for {set phi $phi0} {$phi < $phi1} {set phi [expr $phi + 1]} {
        set point [transform $phi $theta]
        if {$point != {}} then {
            lappend coordList [lindex $point 0] [lindex $point 1]
        } elseif {[llength $coordList] >= 4} {
            .win create line $coordList -fill $color
            set coordList {}
        }
    }
    set point [transform $phi1 $theta]
    lappend coordList [lindex $point 0] [lindex $point 1]

    if {[llength $coordList] >= 4} {
        .win create line $coordList -fill $color -width 2.0
    }
}
# Draw a full-width lat line
proc drawLatLine {phi color} {
	set coordList {}
	# Latitude line; 1 degree intervals
	for {set theta -180} {$theta <= 180} {incr theta} {
		set point [transform $phi $theta]
		if {$point != {}} then {
			lappend coordList [lindex $point 0] [lindex $point 1]
		} elseif {[llength $coordList] >= 4} {
			.win create line $coordList -fill $color
			set coordList {}
		}
	}
	if {[llength $coordList] >= 4} {
		.win create line $coordList -fill $color -width 2.0
	}
}

proc getData {fname name} {
    # Read a file return a list of the lines whose first token begins with $name
    set channel [open $fname]
    set data [read $channel]
    close $channel
    set lines [split $data \n]
    foreach l $lines {
	if {$l == ""} { break }
        set arr [split $l " "]
        set id [lindex $arr 0]
        if {$id == $name} {
            lappend dupeLines $l
        }
    }
    return $dupeLines
}

proc drawData {fname name color} {
    # Draw stripes from $fname indicated by $name in $color
    set data [getData $fname $name]
    drawStripes $data $color
}

proc drawStripes {dataLines color} {
    # Iterate over $dataLines and paint the encoded stripes in $color
    # lines are assumed to be encoded as:
    # Name phi0 phi1 num0 theta00 theta01 [num1 theta10 theta11 [num2 ...]]
    # phi0 and phi1 are + and - boundaries in phi, 
    # numN are piece numbers (chunkId or copyNum)
    # thetaNN are theta boundaries (minTheta and maxTheta for the given piece
    foreach s $dataLines {
        set arr [split $s " "]
        set phi0 [expr [lindex $arr 1]]
        set phi1 [expr [lindex $arr 2]]
        #puts "phi0 $phi0 phi1 $phi1"
        drawLatLine $phi0 $color
        drawLatLine $phi1 $color

	for {set pos 3} {$pos <= [llength $arr]} {incr pos 3} {
            set num [lindex $arr $pos]
            set theta0 [lindex $arr [expr ($pos+1)]]
            set theta1 [lindex $arr [expr ($pos+2)]]
            if {$num == ""} { break }
            #puts "num $num theta $theta0 $theta1"
            drawLonLine $theta0 $phi0 $phi1 $color
            drawLonLine $theta1 $phi0 $phi1 $color
	}
    }
    update
}

proc drawGrid {color} {
    # Draw 10 degree stripes
    for {set phi -80} {$phi <= 80} {incr phi 10} {
        drawLatLine $phi $color
    }
    puts "Done lat"

    # Draw 10 degree longitudes
    for {set theta -180} {$theta <= 180} {incr theta 10} {
        drawLonLine $theta -90 90 $color
    }
    puts "Done long"
}

#drawGrid #222222

drawStripes [getData $plottableFile  "Chunk"] #0000ff
puts "done chunk"
drawStripes [getData $plottableFile "Dupe"] #c00000
puts "done dupe"

# Allow the window and canvas to update
update
puts "Done update"

# Write the image to an EPS file
.win postscript -file gal.eps
puts "Done postscript"