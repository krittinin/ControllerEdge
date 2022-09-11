# Read me

This project is an preliminary implementaion of experiment in the publication, [Empirical Study of Low-Latency Network Model with Orchestrator in MEC ]([10.1587/transcom.2020NVP0005](https://doi.org/10.1587/transcom.2020NVP0005))


# Component Summary
- Edge: The edge computing server. It process each workload upond the controller 
- Source: The client, sends a workload to edge computing system.
- Controller: The central unit to decide accept or reject each request from a client updon a pre-define policy.

# Concept
- Client send a simple workload to solve a puzzel (Sudoku?) to a controller
- Controller periodically measures latency, resouce of each edge server,
- Controller recieves a request from cliten, then select the destination server in the system if the certain conditeions are met. Otherwise reject the request.
- Controller replies to cliend
- If accept, client start communicate to desiaged server.   

# Note
UDP ping is for measure the latency in the research, not an component of this project

