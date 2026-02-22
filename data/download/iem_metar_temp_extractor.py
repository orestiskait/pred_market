"""Download ASOS/METAR from Iowa Environmental Mesonet (IEM) and extract temps from METAR text.

Data source: https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py

Standalone script that downloads ASOS data for a hardcoded station/date,
parses the raw METAR text to extract:
  - Main temp (from XX/YY group)
  - Detailed temp (from RMK T-group, 0.1°C precision)
  - Past 6-hour high (from RMK 1-group)
  - Past 24-hour high (from RMK 4-group)

Writes CSV to data/iem_metar_extracted/. Useful for manual METAR parsing validation.
"""

#!/usr/bin/env python3
import csv
import datetime
import io
import re
import sys
import urllib.request
from pathlib import Path


def c_to_f(c):
    if c is None:
        return ""
    return f"{(c * 9/5) + 32:.2f}"


def format_c(c):
    if c is None:
        return ""
    return f"{c:.2f}"


def process_data(url, output_path):
    print(f"Downloading data from IEM: {url}")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        content = response.read().decode('utf-8')

    reader = csv.DictReader(io.StringIO(content))
    original_fieldnames = reader.fieldnames if reader.fieldnames else []

    if not original_fieldnames:
        print("Error: Downloaded CSV has no header or is empty.", file=sys.stderr)
        return

    extracted_cols = [
        'main_temp_f', 'detailed_temp_f',
        'main_temp_c', 'detailed_temp_c',
        'past_6hr_high_f', 'past_6hr_high_c',
        'past_24hr_high_f', 'past_24hr_high_c'
    ]

    prefix_cols = ['station', 'valid']

    remaining_orig = [
        col for col in original_fieldnames
        if col.lower() not in (['metar', 'tmpf', 'tmpc'] + [c.lower() for c in prefix_cols])
    ]

    fieldnames = prefix_cols + extracted_cols + remaining_orig

    metar_col = next((col for col in original_fieldnames if col.lower() == 'metar'), 'metar')
    if metar_col in fieldnames:
        fieldnames.remove(metar_col)
    fieldnames.append(metar_col)

    print(f"Writing to: {output_path}")
    with open(output_path, 'w', newline='', encoding='utf-8') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            metar_str = row.get('metar', row.get('METAR', ''))

            main_temp_c = None
            main_match = re.search(r'\b(M?\d{2})/(?:M?\d{2}|//)?\b', metar_str)
            if main_match:
                t_str = main_match.group(1)
                if t_str.startswith('M'):
                    main_temp_c = -float(t_str[1:])
                else:
                    main_temp_c = float(t_str)

            detailed_temp_c = None
            past_6hr_high_c = None
            past_24hr_high_c = None

            rmk_match = re.search(r'\bRMK\b(.*)', metar_str)
            if rmk_match:
                rmk_str = rmk_match.group(1)

                t_match = re.search(r'\bT(0|1)(\d{3})(?:0|1)?(?:\d{3})?\b', rmk_str)
                if t_match:
                    sign = 1 if t_match.group(1) == '0' else -1
                    detailed_temp_c = sign * float(t_match.group(2)) / 10.0

                high6_match = re.search(r'\b1(0|1)(\d{3})\b', rmk_str)
                if high6_match:
                    sign = 1 if high6_match.group(1) == '0' else -1
                    past_6hr_high_c = sign * float(high6_match.group(2)) / 10.0

                high24_match = re.search(r'\b4(0|1)(\d{3})(?:0|1)\d{3}\b', rmk_str)
                if high24_match:
                    sign = 1 if high24_match.group(1) == '0' else -1
                    past_24hr_high_c = sign * float(high24_match.group(2)) / 10.0

            row['main_temp_c'] = format_c(main_temp_c)
            row['main_temp_f'] = c_to_f(main_temp_c)
            row['detailed_temp_c'] = format_c(detailed_temp_c)
            row['detailed_temp_f'] = c_to_f(detailed_temp_c)
            row['past_6hr_high_c'] = format_c(past_6hr_high_c)
            row['past_6hr_high_f'] = c_to_f(past_6hr_high_c)
            row['past_24hr_high_c'] = format_c(past_24hr_high_c)
            row['past_24hr_high_f'] = c_to_f(past_24hr_high_c)

            writer.writerow(row)
    print("Done!")


def main():
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=IL_ASOS&station=MDW&data=tmpf&data=tmpc&data=metar&year1=2026&month1=2&day1=19&year2=2026&month2=2&day2=19&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=1&report_type=3&report_type=4"

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # Output to data/iem_metar_extracted/ (project root data/)
    output_dir = Path(__file__).resolve().parent.parent / "iem_metar_extracted"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = f"extracted_temps_{timestamp}.csv"
    output_path = output_dir / output_filename

    process_data(url, str(output_path))


if __name__ == "__main__":
    main()
