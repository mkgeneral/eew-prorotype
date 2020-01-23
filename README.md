# Be warned: it is coming
## __/Earthquake Early Warning/__

## Problem
Currently, it is **impossible to predict** an earthquake. However, recent technological advancements made it **possible** not only **to detect** an earthquake when it begins but **to issue warnings** as well.  

The primary goals of an early earthquake warning are to change the movement of people, vehicles, machinery, and materials that
are in motion. They include:
- Large-Scale Utility Control 
- High-Speed Mass Vehicle Control 
- Industrial Equipment and Process Control
- Industrial Chemical Control 
- Broadcast Notification for General Public Safety
- Activation of Emergency Response Plans

A few seconds of advance warning could possibly help millions to Drop, Cover, and Hold On before heavy shaking begins, as well as increase public preparedness for earthquakes and reduce anxiety.

Here is how it can be made possible:
 
- Less destructive P waves travel faster than the more destructive S waves, and so will arrive first at any given location.
- A dense seismic station network near the earthquake source can quickly detect seismic waves well before the more significant shaking will arrive at more distant population centers.
- Data transmission to a processing center, and distribution to the end user is very fast relative to seismic travel times.

![Image](waves.jpg)

**Query**: Determine if the first earthquake waves have arrived

**Difficulty**: Process sensor data in timely, consistent and reliable manner

**Complexity** in terms of memory/computational complexity/distributed: 
- real-time simultaneous processing of data coming from multiple seismic sensors 
- data produced by different sensors should not be mixed up 
- order of time points should be preserved.

How would you update your results when you get more data: Having more sensors sending data will make computations more conclusive and precise (the closer to the epicenter the higher the acceleration)

![Image](pipeline.jpg)