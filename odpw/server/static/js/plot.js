function mapPlot(data,el){
var width = 250,
    height = 100;

var projection = d3.geo.miller()
    .scale(55)
    .translate([width / 1.7, height / 1.5])
    .precision(.2);

var path = d3.geo.path()
    .projection(projection);

//var graticule = d3.geo.graticule();

var svg = d3.select(el).append("svg")
    .attr("width", width)
    .attr("height", height);



d3.json("/static/data/world-50m.json", function(error, world) {
	svg.insert("path", ".graticule")
		.datum(topojson.feature(world, world.objects.land))
		.attr("class", "land")
		.attr("d", path);

	svg.insert("path", ".graticule")
		.datum(topojson.mesh(world, world.objects.countries, function(a, b) { return a !== b; }))
		.attr("class", "boundary")
		.attr("d", path);
	mapData =[]
	getLongLat(data, function(data){
		mapData=data

	svg.selectAll('circle')
		.data(mapData).enter()
			.append('circle')
			.style("fill", "red")
			.attr('cx', function(d) {
				var cx = projection([d[0], d[1]])[0];
				return cx;
			})
			.attr('cy', function(d) {
				var cy = projection([d[0], d[1]])[1];
				return cy;
			})
			.attr('r', function(d) {return Math.sqrt(d[2]) * 2});
		})
			
});

d3.select(self.frameElement).style("height", height + "px");
	
}

function progressRing(value, el){
	var 	width = 130,
		height = 170,
		twoPi = 2 * Math.PI; 
	formatPercent = d3.format(".0%");
	
	var arc = d3.svg.arc()
		.innerRadius(40)
		.outerRadius(50)
		.startAngle(0);

	var svg = d3.select(el).append("svg")
		.attr("width", width)
		.attr("height", height)
		.append("g")
		.attr("transform", "translate(" + width / 2 + "," + height / 2 + ")")

	var meter = svg.append("g")
		.attr("class", "season-progress");

	var background = meter.append("path")
		.datum({endAngle: twoPi})
		.style("fill", "#ddd")
		.attr("d", arc);

	var foreground = meter.append("path")
		.datum({endAngle:0})
		.style("fill", "orange")
		.attr("class", "foreground")
		.attr("d", arc);

	foreground.transition()
	    .duration(1000)
	    .ease("linear")
	    .attrTween("d", function(d) {
	               var interpolate = d3.interpolate(d.endAngle, twoPi * value)
	               return function(t) {
	                  d.endAngle = interpolate(t);
	                  return arc(d);
	               }  
	            });

	  var text =  meter.append("text")
	    .attr("text-anchor", "middle")
	    .attr("dy", ".35em")
	    .attr("font-size", "24")
	    .text(formatPercent(value));
	
}

function fetchSuccessPlot(data, el){
	var margin = {top: 30, right: 40, bottom: 40, left: 50},
		width = 800 - margin.left - margin.right,
		height = 200 - margin.top - margin.bottom;
	
	var parseDate = d3.time.format("%Y-%W-%w").parse;
	
	var x = d3.time.scale().range([0, width]);
	
	var y = d3.scale.linear()
		.rangeRound([height, 0]);

	var xAxis = d3.svg.axis()
		.scale(x)
		.orient("bottom")
		.tickFormat('')
		.ticks(15)
		//d3.time.format("W%W-%y") 

	var yAxis = d3.svg.axis()
		.scale(y)
		.orient("left")
		.ticks(4);	
	
	x.domain(d3.extent(data, function(d) { console.log(parseDate(d.date));return parseDate(d.date); }));
	x.domain([d3.min(data, function(d) { return d3.time.hour.offset(parseDate(d.date),-72)}), d3.max(data, function(d) { return d3.time.hour.offset(parseDate(d.date),72) })]);
	y.domain([0, d3.max(data, function(d) { return d.total; })]);

	var svg = d3.select(el).append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
			.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
		
	svg.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(0," + (height+5) + ")")
		.call(xAxis);

	svg.append("g")
		.attr("class", "y axis")
		.attr("transform", "translate(-5," + (0) + ")")
		.call(yAxis)

	var barw = 10
	var bar = svg.selectAll("bar")
		.data(data).enter()
			.append("rect")
			.attr("class", "total")
			.attr("x", function(d) { return x(parseDate(d.date))-6; })
			.attr("y", function(d) { return y(d.total); })
			.attr("height", function(d) { return height - y(d.total); })
			.attr("width", barw - 1);
			
	var bar = svg.selectAll("bar1")
		.data(data).enter()
			.append("rect")
			.attr("class", "ok")
			.attr("x", function(d) { return x(parseDate(d.date))-6; })
			.attr("y", function(d) { return y(d.fetched); })
			.attr("height", function(d) { return height - y(d.fetched); })
			.attr("width", barw - 1)
			.style("fill", "steelblue");
	
	var insertLinebreaks = function (d) {
	    var el = d3.select(this);
	    console.log("D: "+d)
	    format = d3.time.format("W%W %Y")
	    var df = format(d)
	    console.log(d+"-"+df)
	    var words = df.split(' ');

	    el.text('');

	    for (var i = 0; i < words.length; i++) {
	        var tspan = el.append('tspan').text(words[i]);
	        if (i > 0)
	            tspan.attr('x', 0).attr('dy', '12');
	    }
	};

	svg.selectAll('g.x.axis g text').each(insertLinebreaks);
}


function portalSoftware( data,el) {
	var total =0;
	for(var i=0; i <data.length;i++)
		total+=data[i].value
	var width = 200,
		height = 100,
		radius = Math.min(width, height) / 2;
	var svg = d3.select(el)
		.append("svg")
		.style("width", width)
		.style("height", height)
		.append("g")

	svg.append("g")
		.attr("class", "slices");
	svg.append("g")
		.attr("class", "labels");
	svg.append("g")
		.attr("class", "lines");

	

	var pie = d3.layout.pie()
		.sort(null)
		.value(function(d) {
			return d.value;
		});

	var arc = d3.svg.arc()
		.outerRadius(radius * 0.8)
		.innerRadius(radius * 0.6);

	var outerArc = d3.svg.arc()
		.innerRadius(radius * 0.9)
		.outerRadius(radius * 0.9);

	svg.attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

	var key = function(d) {
		return d.data.key;
	};

	var color = d3.scale.ordinal()
		.domain(["Lorem ipsum", "dolor sit", "amet", "consectetur", "adipisicing", "elit", "sed", "do", "eiusmod", "tempor", "incididunt"])
		.range(["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"]);

	change(data);

	d3.select(".randomize")
		.on("click", function() {
			change(randomData());
		});


	function change(data) {

		/* ------- PIE SLICES -------*/
		var slice = svg.select(".slices").selectAll("path.slice")
			.data(pie(data), key);

		slice.enter()
			.insert("path")
			.style("fill", function(d) {
				return color(d.data.key);
			})
			.attr("class", "slice");

		slice
			.transition().duration(1000)
			.attrTween("d", function(d) {
				this._current = this._current || d;
				var interpolate = d3.interpolate(this._current, d);
				this._current = interpolate(0);
				return function(t) {
					return arc(interpolate(t));
				};
			})

		slice.exit()
			.remove();

		/* ------- TEXT LABELS -------*/

		var text = svg.select(".labels").selectAll("text")
			.data(pie(data), key);

		text.enter()
			.append("text")
			.attr("dy", ".35em")
			.text(function(d) {
				return d.data.key;
			});

		function midAngle(d) {
			return d.startAngle + (d.endAngle - d.startAngle) / 2;
		}

		text.transition().duration(1000)
			.attrTween("transform", function(d) {
				this._current = this._current || d;
				var interpolate = d3.interpolate(this._current, d);
				this._current = interpolate(0);
				return function(t) {
					var d2 = interpolate(t);
					var pos = outerArc.centroid(d2);
					pos[0] = radius * (midAngle(d2) < Math.PI ? 1 : -1);
					return "translate(" + pos + ")";
				};
			})
			.styleTween("text-anchor", function(d) {
				this._current = this._current || d;
				var interpolate = d3.interpolate(this._current, d);
				this._current = interpolate(0);
				return function(t) {
					var d2 = interpolate(t);
					return midAngle(d2) < Math.PI ? "start" : "end";
				};
			});

		text.exit()
			.remove();

		svg.select(".labels").append("text")
			.attr("dy", "0.2em")
			.attr("x", "-0.5em")
			.text(total)
		/* ------- SLICE TO TEXT POLYLINES -------*/

		var polyline = svg.select(".lines").selectAll("polyline")
			.data(pie(data), key);

		polyline.enter()
			.append("polyline");

		polyline.transition().duration(1000)
			.attrTween("points", function(d) {
				this._current = this._current || d;
				var interpolate = d3.interpolate(this._current, d);
				this._current = interpolate(0);
				return function(t) {
					var d2 = interpolate(t);
					var pos = outerArc.centroid(d2);
					pos[0] = radius * 0.95 * (midAngle(d2) < Math.PI ? 1 : -1);
					return [arc.centroid(d2), outerArc.centroid(d2), pos];
				};
			});

		polyline.exit()
			.remove();
	};

}



function getLongLat(data, callback){
	var map=[
		// [longitude, latitude, num_years, location_string]
		[ -55.765835, -32.522779,3, "uruguay"],
		[ 13.3333,47.3333, 10, "Austria"]
	];
	
	var dd= d3.csv("static/data/countries.csv", function(error, d) {
		var c=0
		var result=[]
		for(var i =0; i< d.length; i++){
			
			if(d[i].name in data){
				result[c]=[
					d[i].longitude,
					d[i].latitude,
					data[d[i].name], 
					d[i].name ]
				c+=1
			}
		}
		callback(result)
	})	
};