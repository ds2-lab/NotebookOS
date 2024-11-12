import grequests
import requests
import json

# Base URL
dashboard_url = "http://localhost:3000/api/dashboards/uid/ddx4gnyl0cmbka"
resp = requests.get(dashboard_url)
dashboard_json = json.loads(resp.content)
panels = dashboard_json["dashboard"]["panels"]

scale = 4
width = 1920
height = 1080
skip_ids = {}

urls = []
paths = []
backup_paths = []

pool_size = 4

def get_panels(d):
    for panel in d:
        panel_id = panel["id"]
        panel_title = panel["title"]

        if panel_id in skip_ids:
            print(f"Skipping panel #{panel_id} '{panel_title}'")
            return

        if "panels" in panel:
            print(f"Recursing on panel #{panel_id} '{panel_title}'")
            get_panels(panel["panels"])

        url = f"http://localhost:3000/render/d-solo/ddx4gnyl0cmbka?scale={scale}&from=2024-11-01T17:23:33.349Z&to=2024-11-01T19:07:37.517Z&timezone=browser&var-local_daemon_id=$__all&refresh=5s&panelId=panel-{panel_id}&__feature.dashboardSceneSolo&width={width}&height={height}&tz=America%2FNew_York"

        # Output file path
        output_path = f"./async_output/{panel_title}_panelID={panel_id}.png"
        backup_output_path = f"./async_output/panel_{panel_id}.png"

        urls.append(url)
        paths.append(output_path)
        backup_paths.append(backup_output_path)

get_panels(panels)

print(f"Asynchronously getting {len(urls)} panels.")

reqs = [grequests.get(u) for u in urls]

for index, response in grequests.imap_enumerated(reqs, stream = True, size = pool_size):
    # Check if the request was successful
    response.raise_for_status()

    output_path = paths[index]
    backup_output_path = backup_paths[index]

    # Write the content to a file in binary mode
    try:
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"Image successfully downloaded: {output_path}")
    except:
        with open(backup_output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"Image successfully downloaded: {backup_output_path}")