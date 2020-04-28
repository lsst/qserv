var img1 = document.getElementById("img1");
var img2 = document.getElementById("img2");
var img3 = document.getElementById("img3");

var theta = 0.0;

function frame() {
    img1.style.top = (25 + 25*Math.sin(theta + 0.0)) + "px";
    img2.style.top = (25 + 25*Math.sin(theta + 0.5)) + "px";
    img3.style.top = (25 + 25*Math.sin(theta + 1.0)) + "px";
    theta = theta + .02;
}

setInterval(frame, 5);
