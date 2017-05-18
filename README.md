![Corda](https://www.corda.net/wp-content/uploads/2016/11/fg005_corda_b.png)

# Module eventbookings

The Event Bookings app tries to keep the following parties in sync about the state of their restaurant reservation,
organised live entertainment etc:
 
 * The people who actually want to attend.
 * The people who paid for it (may be different in case of business entertainment).
 * The agency, if any, who handled the booking.
 * The actual provider of the event.

This app is inspired by a mess I encountered with my girlfriend in Barcelona where we booked a flamenco dinner night 
through a German online booking site, only to discover when we turned up that the restaurant had no idea who we were. 
After nearly getting kicked out I was able to examine the screen of their booking software and noticed one of our three
different booking codes was present under the "name" column of a booking that claimed to be by "Julia", who nobody
had heard of. Luckily the night was saved and we got our dinner and flamenco.

Keeping people in sync as bookings are made and changed seems like the kind of thing a global ledger is useful for. So
let's try and build such an app.