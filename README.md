# Retviews Data Pipeline showcase

This project implements a simple data pipeline that retrieves the following information:

 - Purchase Order lines.
 - Comments made by buyers.
 - On-arrival Quality Notifications
 
And combines it to extract some KPIs at the supplier, area and buyer level:

 - On time delivery rate.
 - Nº of late lines.
 - Nº comments following guidelines.
 - Nº of rejections.


 ## Purchase orders
 Each line in the source file is a single PO-Position-Event.
        - PO: Purchase Order.
        - Position: a Part Number (PN) being purchased with some quantity
        - Event: a delivery event, defined by some quantity of the PN scheduled to be delivered at some date.
Lines which are already delivered are discarded.
The program to which the PO belongs is determined by it's PEP, and the area and buyer are determined by the supplier.
This information is retrieved from closed lists.

### Comments
Each buyer is provided with a file with their assigned PO's, and must provide updated comments about their status.
Comments are validated using reg expressions defined in luigi.cfg. If the PO was created less than a month ago, the comment (or lack of) is validated).

Each reg exp is defined as a dict:

 - name: designation of the type of comment
 - pattern: reg exp pattern
 - validity: number of days in which the comment is valid

The comment date is the first capturing group in the reg exp pattern, and this is compared with the date parameter.

## Quality Notifications
Quality notifications (QN) are incidences detected on the Part Numbers upon delivery. The incidence can affect only part of the delivery.

## KPIs
The final KPI's are calculated and stored at the supplier, area and buyer level.