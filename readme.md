> [!WARNING]
> This server is in active development and not currently intended to be used in a production environment.

# Preaching Partner Server

This repo is the companion server which the mobile application Preaching Partner connects to.

Preaching Partner is a mobile application for managing a congregation of Jehovah's Witnesses' territory. It will help track when a map was last worked, which addresses have requested a do not call status, and which groups are working each map.
(https://github.com/SilentFlag/preaching-partner)

## Features 

Much of the server is still non-functional, however I am building with several clear goals in mind.
- Easy to setup
- Each congregation is in complete control of their data
- Bulk import data such as maps, users, and addresses
- Provide a connection for the mobile app to send and recieve messages

## Why I am building this

My congregation's current solution for managing territory is functional, but could be improved in several areas. I am building to both demonstrate my programming ability and areas where territory management could be improved.

## Tech Stack and Architecture
 
- **Rust**
Rust is the language I have chosen to build this server with, running the backend logic, networking, and database.
 - **Axum**
 Axum is a routing and request handling library for Rust which I am using to manage incoming requests from both the app and in the future, WebUI for admin tasks.
 - **Sqlite**
Sqlite is not intended for use on servers, however I am building this server to be set up by each congregation or group of congregations rather than a central service. This enables congregations to be in complete control of their own data despite the application only being encrypted in transit, and later, also at rest rather than end-to-end encrypted. Using Sqlite makes the setup much simpler as the server admin does not have to worry about setting up a seperate database server such as PostegreSQL.