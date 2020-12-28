// Uncomment the next line to use the production server
//var serverAddress = "http://raptor.cs.ucr.edu";
var serverAddress = "";

var map;
var baseLayer;
var featureLayer;

/**
 * Populates an HTML select element from an array of items, each with a name and ID
 * @param {*} items 
 * @param {*} selectItem 
 */
function populateSelect(items, selectItem) {
  selectItem.empty();
  for (var i in items) {
    var option = jQuery("<option></option>");
    var item = items[i];
    option.val(item.id);
    option.text(item.name);
    selectItem.append(option);
  }
}

/**
 * Populate the list of raster data
 * @param {*} data 
 */
function populateRasters(data) {
  var rasterSelect = jQuery("#raster-select");
  var layers = data.layers;
  populateSelect(layers, rasterSelect);
}

/**
 * Populate the list of vector layers
 * @param {*} data 
 */
function populateVectors(data) {
  var vectorSelect = jQuery("#vector-select");
  var layers = data.layers;
  populateSelect(layers, vectorSelect);
  // Populate the list of features based on the initial vector selection
  vectorLayerChange();
}

/**
 * When the vector layer changes, this method responds by populating the list of features inside this layer.
 */
function vectorLayerChange() {
  var vectorID = jQuery("#vector-select :selected").val();
  jQuery.get(serverAddress+"/dynamic/vectors/"+vectorID+"/features", populateFeatures);
}

function populateFeatures(data) {
  var featureSelect = jQuery("#feature-select");
  var features = data.features;
  populateSelect(features, featureSelect);
  // Display the first feature
  displayFeature();
}

function handleQueryResults(data) {
  // TODO Convert Kelvin to Fahrneheit if it is temperature dataset
  var min = (data.results.min - 273.15) * 9.0 / 5.0 + 32;
  var max = (data.results.max - 273.15) * 9.0 / 5.0 + 32;
  var avg = (parseFloat(data.results.sum) / data.results.count - 273.15) * 9.0 / 5.0 + 32;
  min = data.results.min;
  max = data.results.max;
  avg = data.results.sum / data.results.count;
  jQuery(".result.min").text(min.toFixed(2));
  jQuery(".result.avg").text(avg.toFixed(2));
  jQuery(".result.max").text(max.toFixed(2));
  jQuery(".result.total_time").text(data.runtime.total_time.toFixed(3));
  jQuery(".result.num_files").text(data.runtime.num_files);
}

function runQuery(event) {
  event.preventDefault();
  jQuery("#end-date").val(jQuery("#start-date").val());
  jQuery.get(serverAddress+"/dynamic/zonalstats", jQuery("#queryform").serialize(), handleQueryResults);
}

function displayFeature() {
  var vectorID = jQuery("#vector-select :selected").val();
  var featureID = jQuery("#feature-select :selected").val();
  var featureURL = serverAddress+'/dynamic/vectors/'+vectorID+'/features/'+featureID+'.geojson';
  var source = new ol.source.Vector({
    url: featureURL,
    format: new ol.format.GeoJSON()
  });

  // Zoom to the selected feature
  source.on("change", function () {
    if (featureLayer.getSource().getState() == 'ready') {
      var extent = featureLayer.getSource().getExtent();
      map.getView().fit(extent, { duration: 1000 });
    }
  });

  featureLayer.setSource(source);
}

jQuery(function () {
  // Initialize the map
  var layers = [
    baseLayer = new ol.layer.Tile({ source: new ol.source.OSM({ crossOrigin: null })}),
    featureLayer = new ol.layer.Vector({
      style: function () {
        return new ol.style.Style({
          stroke: new ol.style.Stroke({ color: "rgba(98, 203, 239, 1)", width: 1 }),
          fill: new ol.style.Fill({ color: "rgba(98, 203, 239, 0.1)" })
        })
      }
    })
  ];

  map = new ol.Map({
    layers: layers,
    view: new ol.View({
      center: ol.proj.fromLonLat([-117.3241856, 33.9779584]),
      zoom: 5
    })
  });
  map.setTarget("map");

  // Initialize the vector and raster selectors
  jQuery.get(serverAddress+"/dynamic/rasters", populateRasters);
  jQuery.get(serverAddress+"/dynamic/vectors", populateVectors);
  jQuery("#vector-select").change(vectorLayerChange);
  jQuery("#feature-select").change(displayFeature);

  jQuery("#queryform").submit(runQuery);
});
