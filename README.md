# Flock dashboard
Visualization of when Flock cameras are found

![demo](https://github.com/tsbischof/flock-dashboard/demo.png)

## How it works

The server does the following:

* pulls the minutely updates from openstreetmap
* notes when a flock camera is found and stores the relevant change locally
* sends a notification to all connected clients

## Installing
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python server.py
```

## Using
The client is coded to use `localhost:3000`, change this line to your final hosting location for production.

```
firefox client.html
```

## Customizing

* if you want to pass along labels and other information, modify the method `Node.to_dict`
* fade duration is set in `client.html` in the handler for websocket messages
* websocket server location is set in `client.html`, modify based on your actual hosting

## Repo, issues, pull requests
[Github](https://github.com/tsbischof/flock-dashboard)
