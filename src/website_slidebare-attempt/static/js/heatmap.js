/* heatmap.js
 * Created by N. Christman on 2019/12/13
 */

function heatmap(id, fData,county_topojson,state_topojson,state_fips) {

    //console.log(fData[0].date);
    //console.log("Data:", fData);
    //console.log("County:", county_topojson);
    //console.log("State:", state_topojson);
    //console.log("Fips:", state_fips);

    function typeUnits(u) {
        umap = {pr: 'kg m-2 s-1',
                tasmin: '°K',
                tasmax: '°K',
                vci: '% change',
                tci: '% change',
                vhi: '% change'
            };
        return umap[u]
    }

    function typeName(n) {
        nmap = {pr: 'Precipitation',
                tasmin: 'Min. Temperature',
                tasmax: 'Max. Temperature',
                vci: 'Veg. Condition',
                tci: 'Temp. Condition',
                vhi: 'Veg. Health'
            };
        return nmap[n]
    }

    function createLegend(svg_handle,scale,type,format) {
        // ref: https://d3-legend.susielu.com/
        var legend_class = "legend_" + type;
        svg_handle.append("g")
            .attr("class", legend_class)
            .attr("transform", "translate(20,20)");
  
        var legend = d3.legendColor()
            .labelFormat(d3.format(format))
            .shapeWidth(20)
            .title(typeName(type) + ", " + typeUnits(type))
            .titleWidth(200)
            .orient('vertical')
            .scale(scale);
  
        svg_handle.select("."+legend_class)
            // .attr("")
            .style("font-size", "10px")
            .call(legend);

    }

    function getCountyFill(topo_obj,data_obj,type) {
        // var data = data_obj.get(topo_obj.properties.GEOID); // NC: moved up one level
        var undef_color = "black";

        // console.log(pr_obj)
        if (data_obj != null) { // undefined
            // console.log(pr_obj.value)
            if(type == 'pr') 
                return data_obj.value != -1 ? pr_colors(data_obj.value) : undef_color;
            else if(type == 'tasmin')
                return data_obj.value != -1 ? tasmin_colors(data_obj.value) : undef_color
            else if(type == "tasmax")
                return data_obj.value != -1 ? tasmax_colors(data_obj.value) : undef_color
            else if(type == 'vci')
                return data_obj.value != -1 ? vci_colors(data_obj.value) : undef_color
            else if(type == 'tci')
                return data_obj.value != -1 ? tci_colors(data_obj.value) : undef_color
            else if(type == 'vhi')
                return data_obj.value != -1 ? vhi_colors(data_obj.value) : undef_color
            else
                return "white"
        } else {
            return "white"
        }
    }

    function mouseOverEvent(topo_obj,fips,data_obj,type,subtitle_id) {
        var state_name = fips[topo_obj.properties.STATEFP];
        var county_name = topo_obj.properties.NAME;
        // preset tipText and value
        var tipText = county_name + " County, " + state_name; 
        var value = NaN;
        var date = ""
        // check if data object is undefined, same check for .value
        if(data_obj != null){
            if(data_obj.value != null) {
                // for visualization
                if (data_obj.value == -1)
                    value = NaN;
                else if (data_obj.value == 0)
                    value = data_obj.value.toFixed(1);
                else if (data_obj.value < 0.001 || data_obj.value > 999)
                    value = data_obj.value.toExponential(3);
                else
                    value = data_obj.value.toFixed(3);

                tipText = type + ": " + value + "  " + typeUnits(type)  
                date = ", " + data_obj.date              
            }
        }
        // display the tooltip
        tooltip.transition()
            .duration(200)
            .style("opacity",.9);
        // tooltip will show county, state, value
        tooltip.html(tipText)
            .style("left",(d3.event.pageX + 5) + "px")
            .style("top",(d3.event.pageY - 28) + "px");

        // change the county color
        d3.select(subtitle_id)
            .text(county_name + " County, " + state_name + date);
        // d3.select(this).attr("fill","red"); // NC: moved up one level
    }
    

    // compute total for each state.
    // https://stackoverflow.com/questions/31132813/d3-nest-sum-for-object-array-data
    var prData = d3.map();
    var tasminData = d3.map();
    var tasmaxData = d3.map();
    var vciData = d3.map();
    var tciData = d3.map();
    var vhiData = d3.map();

    var prValues = []
    var tasminValues = []
    var tasmaxValues = []
    var vciValues = []
    var tciValues = []
    // var vhiValues = []
    var dates =[]

    fData.forEach(function (d) {
        if(d.date == "2018-01-08") {
            
            dates.push(Date.parse(d.date))
            
            // values used for color picker / legend
            // If value = -1, then it is NaN... ignore it
            if(d.value.pr != -1) prValues.push(d.value.pr)
            if(d.value.tasmin != -1) tasminValues.push(d.value.tasmin)
            if(d.value.tasmax != -1) tasmaxValues.push(d.value.tasmax)
            if(d.value.vci != -1) vciValues.push(d.value.vci)
            if(d.value.tci != -1) tciValues.push(d.value.tci)

            dates.push(Date.parse(d.date))

            // build the data array
            prData.set(d.geoid, {"date":d.date,"value": d.value.pr}); 
            tasminData.set(d.geoid, {"date":d.date,"value": d.value.tasmin}); 
            tasmaxData.set(d.geoid, {"date":d.date,"value": d.value.tasmax}); 
            vciData.set(d.geoid, {"date":d.date,"value": d.value.vci}); 
            tciData.set(d.geoid, {"date":d.date,"value": d.value.tci}); 
            // vhiData.set(d.geoid, {"date":d.date,"value": d.value.vhi}); 
            
            // prData.set(d.date, {"geoid":d.geoid,"value": d.value.vhi}); 
        }
    });

    // console.log("prData: ",prData)
    // console.log("prValues: ",prValues)
    // console.log("Min:" + Math.min.apply(Math, prValues)); // Min:0
    // console.log("Max:" + Math.max.apply(Math, prValues)); // Max:194

    // Geo D3 example
    // ref: http://duspviz.mit.edu/d3-workshop/mapping-data-with-d3/
    // ref: https://www.jasondavies.com/maps/transition/
    // ref: https://github.com/d3/d3-geo-projection/issues/128
    // Width and Height of the whole visualization
    const width =  600; //600;//window.innerWidth;//975;
    const height = 400;//window.innerHeight;//610;

    // pr svg
    var svg_pr = d3.select("div#pr-container")
        // .append("div")
        // .style("max-width",'800px')
        // Container class to make it responsive.
        .append("svg")
        .attr("preserveAspectRatio", "xMinYMin meet")
        .attr("viewBox", [0, 0, width, height])
        // Class to make it responsive.
        // .classed("svg-content-res", true)
        .attr("width", width)
        // .attr("width", "100%");
        .attr("height", height);
        // FIXME: uncomment when zoom enabled?
        // .on("click", reset);
        // .on("click", function () {
        //     svg_pr.transition()
        //     .attr("x", 250)
        //     // .attr("width", 100)
        //     // .attr("height", 100)
        //     .attr("opacity", 0.5)
        //     // .attr("fill", "red")
        //     .delay(500)
        //     .duration(2500); // Set Duration of 5000ms (5 seconds)
        // });
    var g_pr = svg_pr.append( "g" )
    
    // tasmin svg
    var svg_tasmin = d3.select("div#tasmin-container")
        // Container class to make it responsive.
        .append("svg")
        .attr("preserveAspectRatio", "xMinYMin meet")
        .attr("viewBox", [0, 0, width, height])
        // Class to make it responsive.
        // .classed("svg-content-res", true)
        .attr("width", width)
        .attr("height", height)
        // .on("click", reset);
    var g_tasmin = svg_tasmin.append( "g" )

    // tasmax svg
    var svg_tasmax = d3.select("div#tasmax-container")
        // Container class to make it responsive.
        .append("svg")
        .attr("preserveAspectRatio", "xMinYMin meet")
        .attr("viewBox", [0, 0, width, height])
        // Class to make it responsive.
        // .classed("svg-content-res", true)
        .attr("width", width)
        .attr("height", height)
        // .on("click", reset);
    var g_tasmax = svg_tasmax.append( "g" )

    // vci svg
    var svg_vci = d3.select("div#vci-container")
        // Container class to make it responsive.
        .append("svg")
        .attr("preserveAspectRatio", "xMinYMin meet")
        .attr("viewBox", [0, 0, width, height])
        // Class to make it responsive.
        // .classed("svg-content-res", true)
        .attr("width", width)
        .attr("height", height)
        // .on("click", reset);
    var g_vci = svg_vci.append( "g" )
   
    // tci svg
    var svg_tci = d3.select("div#tci-container")
        // Container class to make it responsive.
        .append("svg")
        .attr("preserveAspectRatio", "xMinYMin meet")
        .attr("viewBox", [0, 0, width, height])
        // Class to make it responsive.
        // .classed("svg-content-res", true)
        .attr("width", width)
        .attr("height", height)
        // .on("click", reset);
    var g_tci = svg_tci.append( "g" )
        
    // define mouseover tooltip
    var tooltip = d3.select("body").append("div").attr("class","tooltip");

    // var Slider with custom axis
    // d3.slider().axis( d3.svg.axis().orient("top").ticks(6) )
    
    /*
    * FIXME: Uncomment to enable zoom
    */
    // var zoom = d3.zoom()
    //     .scaleExtent([1, 8])
    //     .on("zoom", zoomed);

    // create legends for each chart
    var pr_range = ["beige", "green"];
    var pr_ext = d3.extent(prValues)
    var pr_colors = d3.scaleLinear().domain(pr_ext).range(pr_range)
    createLegend(svg_pr,pr_colors,'pr',".1e") // create the legend

    var tasmin_range = ["blue","beige"];
    var tasmin_ext = d3.extent(tasminValues)
    var tasmin_colors = d3.scaleLinear().domain(tasmin_ext).range(tasmin_range)
    createLegend(svg_tasmin,tasmin_colors,'tasmin',".1f") // create the legend
    
    var tasmax_range = ["beige", "orange"];
    var tasmax_ext = d3.extent(tasmaxValues)
    var tasmax_colors = d3.scaleLinear().domain(tasmax_ext).range(tasmax_range)
    createLegend(svg_tasmax,tasmax_colors,'tasmax',".1f") // create the legend
    
    var vci_range = ["beige", "#3DF700"];
    var vci_ext = d3.extent(vciValues)
    var vci_colors = d3.scaleLinear().domain(vci_ext).range(vci_range)
    createLegend(svg_vci,vci_colors,'vci',".4f") // create the legend

    var tci_range = ["beige", "#3DF700"];
    var tci_ext = d3.extent(tciValues)
    var tci_colors = d3.scaleLinear().domain(tci_ext).range(tci_range)
    createLegend(svg_tci,tci_colors,'tci',".4f") // create the legend


    // asynchronously load topojson and data
    d3.queue()
        .defer(d3.json,county_topojson) // county TopoJSON file
        .defer(d3.json,state_topojson)  // state TopoJSON file
        .defer(d3.json,state_fips)  // state fips JSON file        
        .await(ready)

     // update the fill of each SVG of class "incident"
    function update(value) {
        document.getElementById("range").innerHTML="Week " + value;
        // inputValue = month[value];
        // d3.selectAll(".incident")
        //     .attr("fill", dateMatch);
    }

    // callback function for d3.queue() async load
    function ready(error,county_obj,state_obj,state_fips){
        if(error) {
            console.warn(error);
            throw error;
        }

        // // when the input range changes update the value
        // d3.select("#timeslide").on("input", function() {
        //     // update(+this.value);
        //     console.log(this.value)
        //     update(this.value)
        // });

        // Time
        // var dataTime = dates.map(function(d) {
        //     return new Date();
        // });

        // console.log("min",d3.min(dates))
        // console.log("max",d3.max(dates))
        // console.log("length",dates.length)
        // console.log("dates",dates)
        // console.log(d3.range(1,52))

        // var sliderTime = d3
        //     .sliderBottom()
        //     .min(1)
        //     .max(105)
        //     .step(5)
        //     .width(300)
        //     .tickFormat(d3.timeFormat('%W')) // 2019 W23
        //     .tickValues(d3.range(1,105))
        //     .default(1)
        //     .on('onchange', val => {
        //         // d3.select('p#value-time').text(d3.timeFormat('%Y')(val));
        //         // d3.select('p#value-time').text(d3.timeFormat('%Y')(dates[val]));
        //         console.log(dates[val])
        //         return val;
        //     });

        // var gTime = d3
        //     .select('div#slider-time')
        //     .append('svg')
        //     .attr('width', 500)
        //     .attr('height', 100)
        //     .append('g')
        //     .attr('transform', 'translate(30,30)');

        // gTime.call(sliderTime);
        // d3.select('p#value-time').text(d3.timeFormat('%Y')(dates[sliderTime.value()]));
        
        // Use the state TopoJSON to draw high level display of map
        // console.log(state_obj)
        var states = topojson.feature(state_obj, {
            type: "GeometryCollection",
            geometries: state_obj.objects.states.geometries // open topojson > "objects":{"counties": {"geometries" :[{...}]}}
        });

        // projection (fit to states)
        var projection = d3.geoAlbersUsa()
            .fitExtent([ [100,0],[width,height] ], states) 
            // .fitSize([460,580], states) 
            // .fitWidth(460, states) 
            // .fitHeight(580, states) 

        // Use the county TopoJSON to draw counties with values
        // console.log(county_obj)
        // contiguous US counties TopoJSON
        var counties = topojson.feature(county_obj, {
            type: "GeometryCollection",
            geometries: county_obj.objects.counties.geometries // open topojson > "objects":{"counties": {"geometries" :[{...}]}}
        });

        // path for all figures
        var path = d3.geoPath()
            .projection(projection)

        // Used for drawing borders around boundaries for all maps
        var state_mesh = topojson.mesh(state_obj,state_obj.objects.states)
        var county_mesh = topojson.mesh(county_obj,county_obj.objects.counties)

        /* 
         * pr data map
        */
        // d3.select("h3#pr-title")
        //     .text(prData.date);

        svg_pr.append("path")
            .attr("fill", "none")
            .attr("stroke", "white")
            .attr("stroke-linejoin", "round")
            // .datum(topojson.mesh(us, us.objects.layer1, function(a, b) { return a !== b }))
            .attr("d", path(topojson.mesh(county_obj, county_obj.objects.counties, (a, b) => a !== b)));
     
        // pr region shadow
        // TODO: put a shadow around the whole region
        // svg_pr.selectAll("path")
        //     .data(region.features)
        //     .enter()
        //     .append("path")
        //     .attr("class",".region-border")
        //     .attr("d",path)

        svg_pr.selectAll("path")
            .data(counties.features)
            .enter()
            .append("path")
            .attr("class",".state-boundary")
            .attr("d",path)
            // .attr("fill","green");
            .attr("fill", function(d) { 
                var data_obj = prData.get(d.properties.GEOID);
                return getCountyFill(d,data_obj,'pr') 
            })
            .on("mouseover",function(d) {
                var subtitle_id = "#pr-subtitle";
                var type = 'pr'
                // get the data object and statename
                var data_obj =  prData.get(d.properties.GEOID);
                // console.log(pr_obj);
                mouseOverEvent(d,state_fips,data_obj,type,subtitle_id)
                d3.select(this).attr("fill","red");

                ////////// ADD FILTER /////////////
                // // filters go in defs element
                // var defs = svg_pr.append("defs");

                // // create filter with id #drop-shadow
                // // height=130% so that the shadow is not clipped
                // var filter = defs.append("filter")
                //     .attr("id", "drop-shadow")
                //     .attr("height", "130%");

                // // SourceAlpha refers to opacity of graphic that this filter will be applied to
                // // convolve that with a Gaussian with standard deviation 3 and store result
                // // in blur
                // filter.append("feGaussianBlur")
                //     .attr("in", "SourceAlpha")
                //     .attr("stdDeviation", 5)
                //     .attr("result", "blur");

                // // translate output of Gaussian blur to the right and downwards with 2px
                // // store result in offsetBlur
                // filter.append("feOffset")
                //     .attr("in", "blur")
                //     .attr("dx", 5)
                //     .attr("dy", 5)
                //     .attr("result", "offsetBlur");

                // // overlay original SourceGraphic over translated blurred opacity by using
                // // feMerge filter. Order of specifying inputs is important!
                // var feMerge = filter.append("feMerge");

                // feMerge.append("feMergeNode")
                //     .attr("in", "offsetBlur")
                // feMerge.append("feMergeNode")
                //     .attr("in", "SourceGraphic");
                // d3.select(this).style("filter", "url(#drop-shadow)")

            })
            .on("mouseout", function (d) {
                tooltip.transition()
                .duration(400)
                .style("opacity",0);

                d3.select("#pr-subtitle")
                .text("")
                .append('br');//.text(" ");

                d3.select(this)
                    .attr("fill",function(d) { 
                        var data_obj = prData.get(d.properties.GEOID)
                        return getCountyFill(d,data_obj,'pr')
                    });
            });

        // add black boundary around states
        svg_pr.append("path")
            .datum(state_mesh,function(a,b) { return a != b;})
            .attr("class","state-boundary")
            .attr("d",path)
        
       
        svg_pr.append("path")
            .datum(county_mesh,function(a,b) { return a != b;})
            .attr("class","county-boundary")
            .attr("d",path)
         

        /* 
         * tasmin data map
        */
        svg_tasmin.append("path")
            .attr("fill", "none")
            .attr("stroke", "white")
            .attr("stroke-linejoin", "round")
            .attr("d", path(topojson.mesh(county_obj, county_obj.objects.counties, (a, b) => a !== b)));

        svg_tasmin.selectAll("path")
            .data(counties.features)
            .enter()
            .append("path")
            .attr("class",".state-border")
            .attr("d",path)
            .attr("fill", function(d) { 
                var data_obj = tasminData.get(d.properties.GEOID);
                return getCountyFill(d,data_obj,'tasmin') 
            })
            .on("mouseover",function(d) {
                var subtitle_id = "#tasmin-subtitle";
                var type = 'tasmin'
                // get the data object and statename
                var data_obj =  tasminData.get(d.properties.GEOID);
                // console.log(pr_obj);
                mouseOverEvent(d,state_fips,data_obj,type,subtitle_id)
                d3.select(this).attr("fill","red");
            })
            .on("mouseout", function (d) {
                tooltip.transition()
                .duration(400)
                .style("opacity",0);

                d3.select("#tasmin-subtitle")
                .text("")
                .append('br');//.text(" ");
                
                d3.select(this)
                    .attr("fill",function(d) { 
                        var data_obj = tasminData.get(d.properties.GEOID)
                        return getCountyFill(d,data_obj,'tasmin')
                    });
            });
        
        // add black boundary around states
        svg_tasmin.append("path")
            .datum(state_mesh,function(a,b) { return a != b;})
            .attr("class","state-boundary")
            .attr("d",path)
        
        svg_tasmin.append("path")
            .datum(county_mesh,function(a,b) { return a != b;})
            .attr("class","county-boundary")
            .attr("d",path)
        
        /* 
         * tasmax data map
        */
        svg_tasmax.append("path")
            .attr("fill", "none")
            .attr("stroke", "white")
            .attr("stroke-linejoin", "round")
            // .datum(topojson.mesh(us, us.objects.layer1, function(a, b) { return a !== b }))
            .attr("d", path(topojson.mesh(county_obj, county_obj.objects.counties, (a, b) => a !== b)));

        svg_tasmax.selectAll("path")
            .data(counties.features)
            .enter()
            .append("path")
            .attr("class",".state-border")
            .attr("d",path)
            .attr("fill", function(d) { 
                var data_obj = tasmaxData.get(d.properties.GEOID);
                return getCountyFill(d,data_obj,'tasmax') 
            })
            .on("mouseover",function(d) {
                var subtitle_id = "#tasmax-subtitle";
                var type = 'tasmax'
                // get the data object and statename
                var data_obj =  tasmaxData.get(d.properties.GEOID);
                // console.log(pr_obj);
                mouseOverEvent(d,state_fips,data_obj,type,subtitle_id)
                d3.select(this).attr("fill","red");
            })
            .on("mouseout", function (d) {
                tooltip.transition()
                .duration(400)
                .style("opacity",0);

                d3.select("#tasmax-subtitle")
                .text("")
                .append('br');//.text(" ");
                
                d3.select(this)
                    .attr("fill",function(d) { 
                        var data_obj = tasmaxData.get(d.properties.GEOID)
                        return getCountyFill(d,data_obj,'tasmax')
                    });
            });
        
        // add black boundary around states
        svg_tasmax.append("path")
            .datum(state_mesh,function(a,b) { return a != b;})
            .attr("class","state-boundary")
            .attr("d",path)
        
        svg_tasmax.append("path")
            .datum(county_mesh,function(a,b) { return a != b;})
            .attr("class","county-boundary")
            .attr("d",path)
        
        /* 
         * vci data map
        */
        svg_vci.append("path")
            .attr("fill", "none")
            .attr("stroke", "white")
            .attr("stroke-linejoin", "round")
            // .datum(topojson.mesh(us, us.objects.layer1, function(a, b) { return a !== b }))
            .attr("d", path(topojson.mesh(county_obj, county_obj.objects.counties, (a, b) => a !== b)));

            svg_vci.selectAll("path")
            .data(counties.features)
            .enter()
            .append("path")
            .attr("class",".state-border")
            .attr("d",path)
            .attr("fill", function(d) { 
                var data_obj = vciData.get(d.properties.GEOID);
                return getCountyFill(d,data_obj,'vci') 
            })
            .on("mouseover",function(d) {
                var subtitle_id = "#vci-subtitle";
                var type = 'vci'
                // get the data object and statename
                var data_obj =  vciData.get(d.properties.GEOID);
                // console.log(pr_obj);
                mouseOverEvent(d,state_fips,data_obj,type,subtitle_id)
                d3.select(this).attr("fill","red");
            })
            .on("mouseout", function (d) {
                tooltip.transition()
                .duration(400)
                .style("opacity",0);

                d3.select("#vci-subtitle")
                .text("")
                .append('br');//.text(" ");
                
                d3.select(this)
                    .attr("fill",function(d) { 
                        var data_obj = vciData.get(d.properties.GEOID)
                        return getCountyFill(d,data_obj,'vci')
                    });
            });
        
        // add black boundary around states
        svg_vci.append("path")
            .datum(state_mesh,function(a,b) { return a != b;})
            .attr("class","state-boundary")
            .attr("d",path)
        
        svg_vci.append("path")
            .datum(county_mesh,function(a,b) { return a != b;})
            .attr("class","county-boundary")
            .attr("d",path)
        
        /* 
         * tci data map
        */
        svg_tci.append("path")
            .attr("fill", "none")
            .attr("stroke", "white")
            .attr("stroke-linejoin", "round")
            // .datum(topojson.mesh(us, us.objects.layer1, function(a, b) { return a !== b }))
            .attr("d", path(topojson.mesh(county_obj, county_obj.objects.counties, (a, b) => a !== b)));

            svg_tci.selectAll("path")
            .data(counties.features)
            .enter()
            .append("path")
            .attr("class",".state-border")
            .attr("d",path)
            .attr("fill", function(d) { 
                var data_obj = tciData.get(d.properties.GEOID);
                return getCountyFill(d,data_obj,'tci') 
        })
        .on("mouseover",function(d) {
            var subtitle_id = "#tci-subtitle";
            var type = 'tci'
            // get the data object and statename
            var data_obj =  tciData.get(d.properties.GEOID);
            // console.log(pr_obj);
            mouseOverEvent(d,state_fips,data_obj,type,subtitle_id)
            d3.select(this).attr("fill","red");
        })
        .on("mouseout", function (d) {
            tooltip.transition()
            .duration(400)
            .style("opacity",0);

            d3.select("#tci-subtitle")
            .text("")
            .append('br');//.text(" ");
            
            d3.select(this)
                .attr("fill",function(d) { 
                    var data_obj = tciData.get(d.properties.GEOID)
                    return getCountyFill(d,data_obj,'tci')
                });
        });
    
    // add black boundary around states
    svg_tci.append("path")
        .datum(state_mesh,function(a,b) { return a != b;})
        .attr("class","state-boundary")
        .attr("d",path)
    
        svg_tci.append("path")
        .datum(county_mesh,function(a,b) { return a != b;})
        .attr("class","county-boundary")
        .attr("d",path)

        /*
        * FIXME: Uncomment to enable zoom
        */      
        // svg_pr.call(zoom);        
    }

    function reset() {
        svg_pr.transition().duration(750).call(
            zoom.transform,
            d3.zoomIdentity,
            d3.zoomTransform(svg_pr.node()).invert([width / 2, height / 2])
        );
    }

    function zoomed() {
        const {transform} = d3.event;
        svg_pr.attr("transform", transform);
        svg_pr.attr("stroke-width", 1 / transform.k);
    }

    function clicked(d) {
        const [[x0, y0], [x1, y1]] = path.bounds(d);
        d3.event.stopPropagation();
        svg_pr.transition().duration(750).call(
            zoom.transform,
            d3.zoomIdentity
                .translate(width / 2, height / 2)
                .scale(Math.min(8, 0.9 / Math.max((x1 - x0) / width, (y1 - y0) / height)))
                .translate(-(x0 + x1) / 2, -(y0 + y1) / 2),
            d3.mouse(svg_pr.node())
        );
    }

}