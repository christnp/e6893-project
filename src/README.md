# Example Package

This is a simple example package. You can use
[Github-flavored Markdown](https://guides.github.com/features/mastering-markdown/)
to write your content.

### TODO: 
- generate distro > https://packaging.python.org/tutorials/packaging-projects/


# QGIS
Add the lines for one of the repositories to your /etc/apt/sources.list:
deb https://qgis.org/ubuntu bionic main
deb-src https://qgis.org/ubuntu bionic main

(In case of keyserver errors add the qgis.org repository public key to your apt keyring, type:)
wget -O - https://qgis.org/downloads/qgis-2019.gpg.key | gpg --import
gpg --fingerprint 51F523511C7028C3
gpg --export --armor 51F523511C7028C3 | sudo apt-key add -

sudo apt-get update
sudo apt-get install qgis qgis-plugin-grass

Open US county shape file (from US Census) in QGIS. 
Highlight only desired area (I chose Contiguous US) OR use the "select features by expression" option
Click Layer > Save As and Choose GeoJSON, check "selected features only" and save new file

Install TopoJSON (npm install topojson-server)
Convert GeoJson to TopopJson --> geo2topo cont_us_states.geojson > cont_us_states.json

