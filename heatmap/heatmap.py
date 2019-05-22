import os
from PIL import Image
from h3 import h3
from utils.connection_helper import get_redis_connection, get_ucloud_file_manager, get_google_map_js_api_key
import time

minLat, maxLat, minLng, maxLng = 6.391823, 6.697766, 3.058968, 3.473307
hotmap_format = "hotmap:%s"
hot_map_level = 8

html_part1 = '''
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Heatmaps</title>
    <style>
      /* Always set the map height explicitly to define the size of the div
       * element that contains the map. */
      #map {
        height: 100%;
      }
      /* Optional: Makes the sample page fill the window. */
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
      #floating-panel {
        position: absolute;
        top: 10px;
        left: 25%;
        z-index: 5;
        background-color: #fff;
        padding: 5px;
        border: 1px solid #999;
        text-align: center;
        font-family: 'Roboto','sans-serif';
        line-height: 30px;
        padding-left: 10px;
      }
      #floating-panel {
        background-color: #fff;
        border: 1px solid #999;
        left: 25%;
        padding: 5px;
        position: absolute;
        top: 10px;
        z-index: 5;
      }
    </style>
  </head>

  <body>
    <div id="map"></div>
    <script>

      // This example requires the Visualization library. Include the libraries=visualization
      // parameter when you first load the API. For example:
      // <script src="https://maps.googleapis.com/maps/api/js?key=YOUR_API_KEY&libraries=visualization">

      var map, heatmap;

      function initMap() {
        map = new google.maps.Map(document.getElementById('map'), {
          zoom: 10,
          center: {lat: 6.552566, lng: 3.337010},
        });

        heatmap = new google.maps.visualization.HeatmapLayer({
          data: getPoints(),
          map: map
        });

        heatmap.set('radius', 10);
      }

      // Heatmap data: 500 Points
      function getPoints() {
        return [
'''

data_format = '''
          {location: new google.maps.LatLng(%f, %f), weight: %d},
'''

html_part2 = '''
        ];
      }
    </script>
    <script async defer
        src="https://maps.googleapis.com/maps/api/js?language=en-US&key=%s&libraries=visualization&callback=initMap">
    </script>
  </body>
</html>
''' % get_google_map_js_api_key()


def get_data(hex_addrs):
    rds = get_redis_connection()
    pipe = rds.pipeline(transaction=False)
    data = []
    counter = 0
    for elem in hex_addrs:
        pipe.hgetall(hotmap_format % elem)
        if counter % 256 == 0:
            data += pipe.execute()
    data += pipe.execute()
    return data


def data2goolemap(hex_res, data, t):
    tmp_dict = {
        'o': b'o',
        'oc': b'oc',
        'd': b'd',
    }
    tt = tmp_dict[t]
    tmp_zip_data = zip(hex_res, data)
    tmp_zip_data = [x for x in tmp_zip_data if x[1] != {} and tt in x[1] and int(x[1][tt]) > 0]
    print(len(tmp_zip_data))
    tmp_zip_data.sort(key=lambda x: int(x[1][tt]))
    s = html_part1
    for x in range(len(tmp_zip_data)):
        tmp_loc = h3.h3_to_geo(tmp_zip_data[x][0])
        s += data_format % (tmp_loc[0], tmp_loc[1], int(tmp_zip_data[x][1][tt]))
    s += html_part2
    return s


def reshape_img(img_path):
    img = Image.open(img_path)
    x_range = 460
    y_range = 230
    bbox = (img.size[0] / float(2) - x_range / float(2), img.size[1] / float(2) - y_range / float(2),
            img.size[0] / float(2) + x_range / float(2), img.size[1] / float(2) + y_range / float(2))
    cropped = img.crop(bbox)
    cropped.thumbnail((360, 180))
    cropped.show()
    tmp_arr = img_path.split(".")
    tmp_arr[-2] += "_cropped"
    file_name = ".".join(tmp_arr)
    cropped.save(file_name)
    return file_name


def upload_img(img_path):
    file_manager = get_ucloud_file_manager()
    public_bucket = 'oride-resource'
    remote_path = 'heatmap/heatmap_%s.jpg' % time.strftime("%Y%m%d%H%M%S", time.localtime(int(time.time())))
    ret, resp = file_manager.putfile(public_bucket, remote_path, img_path)
    assert resp.status_code == 200
    rds = get_redis_connection()
    rds.set("heatmap_key", remote_path, 86400 * 365)
    rds = get_redis_connection('redis_test')
    rds.set("heatmap_key", remote_path, 86400 * 365)


def generate_heat_map(**op_kwargs):
    # get data
    hot_map_type = 'o'
    tmp = {u'type': u'Polygon', u'coordinates':
        [[[minLat, minLng], [maxLat, minLng], [maxLat, maxLng], [minLat, maxLng]]]}
    hex_res = list(h3.polyfill(geo_json=tmp, res=hot_map_level))
    file_dir = "/tmp/heat_map"
    data = get_data(hex_res)
    i1 = data2goolemap(hex_res, data, hot_map_type)

    # generate html file
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    f1 = open("%s/%s.html" % (file_dir, hot_map_type), "w")
    f1.write(i1)
    f1.close()

    # convert html to png
    cmd = '''
	xvfb-run --server-args="-screen 0 1024x768x24" CutyCapt --min-width=900 --min-height=400 --url=file:{file_dir}/{filename}.html --out={file_dir}/{filename}.png --delay=10000
	'''.format(file_dir=file_dir, filename=hot_map_type)
    os.system(cmd.strip())

    # crop to smaller png
    cropped_file = reshape_img("%s/%s.png" % (file_dir, hot_map_type))

    # compress png to jpg
    upload_file = '%s/%s.jpg' % (file_dir, hot_map_type)
    cmd = 'guetzli --quality 90 %s %s' % (cropped_file, upload_file)
    os.system(cmd)

    # ensure that file is not bad
    assert os.path.getsize(upload_file) >= 10 * 1024

    # upload file
    upload_img(upload_file)
