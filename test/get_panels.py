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
        output_path = f"./output/{panel_title}_panelID={panel_id}.png"
        backup_output_path = f"./output/panel_{panel_id}.png"

        # Send the GET request
        try:
            print(f"Getting panel {panel_id}")
            response = requests.get(url, stream=True)
            # Check if the request was successful
            response.raise_for_status()

            # Write the content to a file in binary mode
            try:
                with open(output_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
            except:
                with open(backup_output_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

            print(f"Image successfully downloaded: {output_path}")
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"An error occurred: {err}")

get_panels(panels)