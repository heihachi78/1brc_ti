# 1brc_ti
 
This is a solution to the one billion row challenge in go, which can be found here: https://github.com/gunnarmorling/1brc

This solution processes the file in 9seconds on a 8 core processor with 64GB of rams and a 7GB/s SSD.

## Known issues
- Sorting is not working properly, go built in function is doing some strange stuff.
- It will work only on windows with cr+lf (13+10) line end characters.
