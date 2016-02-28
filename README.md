# Reactive FX Activator Template

This tutorial shows how Akka Streams can be used to solve real world problems

Reactive FX is a sample application that provides a context for the tutorial. It represents a Forex dealer dashboard, which interfaces with a remote pricing engine and allows users to subscribe to price streams through the web interface.

![screenshot](https://raw.githubusercontent.com/intelix/activator-reactive-fx/master/tutorial/img/screenshot1.png "screenshot")


The topics covered in this tutorial are:

* How to implement a light-weight, fast and scalable websocket endpoint
* How to create inter-process back-pressured streams
* How to subscribe to live streams of data
* How to deal with very fast data sources
* How to automatically recover end-to-end flow, with a minimal impact on the user experience
* How to scale the application to improve availability or in response to a higher demand

## Run in Activator

Clone the repo and open from the Activator UI

## Run from SBT

Clone the repo, navigate to the folder and run `sbt run`
