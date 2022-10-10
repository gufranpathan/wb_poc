# Parse Voter Rolls in Local Languages

Scrapes voter information off the voter rolls. Local languges supported (tested for Bengali).

Run using:

````
python -m --assembly 1 --start_part 1 --end_part 250 --page 1 --lang BEN
````

```
Arguments:

"-a", "--assembly": Assembly Number
"-t", "--part": Part / Booth Number (use if parsing only one part / booth)
"-g", "--page": Page Number (use if parsing only one page)
"-d", "--dpi": Resolution to use for OCR
"-l", "--lang": Three-letter language code as specified in Tesseract models
"-s", "--start_part": Starting range of part number (use if parsing a rane of parts / booths)
"-e", "--end_part": Ending range of part number (use if parsing a rane of parts / booths)
```
