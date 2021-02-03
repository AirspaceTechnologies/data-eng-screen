# Data Engineer Screen

### Problem Space
At Airspace, we have a number of microservices that create data. We need this data to be extracted and transformed into tables and views that make sense to end users and are easy to access. Taking disparate data and denormalizing it is a very common practice and candidates should have experience with this process.
### Goals
The goal of this screen is to *programmatically* load the provided tables into a cloud hosted database (a free heroku db is totally fine), and then create a denormalized table or view with the following columns (note: all times in the CSV files are in UTC and not local time):
```
order_id, company_id, origin_city, destination_city, pick_up_time_local, delivery_time_local, minutes_to_pickup, order_type, total_drive_distance
```

### Column definitions
- **order_id**: the id of the order
- **company_id**: the id of the company for which the order was performed
- **origin_city**: the city where the order was picked up
- **destination_city**: the city where the order was delivered
- **pick_up_time_local**: the time of pickup in the time zone of the pickup location
- **delivery_time_local**: the time of delivery in the time zone of the delivery location
- **minutes_to_pickup**: how many minutes between order creation and pickup
- **order_type**: drive, hfpu, or nfo. A drive has a single driving segment, hold for pickup(hfpu) will have just a drive and a flight, and nfo will have a drive, flight, and another drive.
- **total_drive_distance**: sum of the distance in miles of all of the drives associated with an order

### Other definitions
- **NFO**: fastest type of shipping. This is where a driver will pick something up, put it on a plane and the a driver will recover from the destination airport and perform the final delivery.
- **order**: top level object
- **delivery_route_segment**: the modes of transit for an order. One order will have between one and three segments depending on the type of order. For instance, a drive will only have a DrivingSegment while and NFO will have A DrivingSegment, FlyingSegment, and another DrivingSegment
- **driving_search**: Third party response with routing information for a DrivingSegment

### Additional requirement
A query should be provided that will return the order with the longest distance between the pickup and delivery locations. 

### Submision
DB credentials, code used to load tables & build denormalized table/view, as well as required querie(s) to satisfy the additional requirement. 
