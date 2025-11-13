# NBA API Data Pipeline Documentation

## Overview

This document describes the NBA API data pipeline that extracts game log data directly from the NBA API instead of web scraping Basketball Reference. The pipeline reuses existing transformation logic and integrates seamlessly with the current DVC workflow.

## Architecture

The NBA API pipeline consists of three main stages:

1. **Extraction** (`nba_api_gamelog_data_acquisition`)
2. **Transformation** (`nba_api_gamelog_transformation`)
3. **Loading** (`nba_api_output_to_s3`)

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│         STAGE 1: NBA API DATA EXTRACTION                    │
├─────────────────────────────────────────────────────────────┤
│ Input:  NBA API (via basketball-reference-webscrapper)      │
│ Output: pipeline_output/nba_api_gamelog/                    │
│         gamelog_nba_api_{season}_all.csv                    │
│                                                              │
│ Columns (24):                                                │
│   - id_season, game_nb, game_date, extdom, tm, opp, results│
│   - pts_tm, fg_tm, fga_tm, fg_prct_tm, 3p_tm, 3pa_tm,     │
│     3p_prct_tm, ft_tm, fta_tm, ft_prct_tm                  │
│   - orb_tm, trb_tm, ast_tm, stl_tm, blk_tm, tov_tm, pf_tm  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│         STAGE 2: TRANSFORMATION                              │
├─────────────────────────────────────────────────────────────┤
│ Input:  gamelog_nba_api_{season}_all.csv                    │
│ Output: pipeline_output/nba_api_transformed/                │
│         gamelog_nba_api_transformed_{season}.csv            │
│                                                              │
│ Processing:                                                  │
│   1. Filter by season date range (Oct-Jun)                  │
│   2. Self-merge on [game_date, tm, opp]                     │
│   3. Create opponent statistics (_opp columns)              │
│   4. Validate 41-column output structure                    │
│                                                              │
│ Columns (41):                                                │
│   - All 24 original columns with _tm suffix                 │
│   - 17 opponent columns with _opp suffix                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│         STAGE 3: LOADING TO S3                               │
├─────────────────────────────────────────────────────────────┤
│ Input:  gamelog_nba_api_transformed_{season}.csv            │
│ Output: s3://foreshadownba/data-engineering-pipeline-output/│
│         nba-api-gamelogs/                                    │
└─────────────────────────────────────────────────────────────┘
```

## Key Differences from Basketball Reference Pipeline

### Extraction

**Basketball Reference (Web Scraping)**
- Uses `WebScrapBasketballReference` class
- Scrapes HTML from Basketball-Reference.com
- Subject to website changes and rate limiting
- Output: `gamelog_{season}_{team}.csv`

**NBA API (Direct API)**
- Uses `WebScrapNBAApi` class
- Fetches JSON from stats.nba.com API
- More reliable and faster
- Output: `gamelog_nba_api_{season}_{team}.csv`

### Schema

**Good News!** The `basketball-reference-webscrapper` package's `WebScrapNBAApi` class already formats NBA API data to match the Basketball Reference schema. This means:

- **No mapper function is needed!**
- Both sources output 24 columns with `_tm` suffix
- Column names and data types are identical
- Existing transformation logic works without modification

### Transformation

The transformation logic is identical for both pipelines:

1. Date filtering (season boundaries)
2. Self-merge to create opponent statistics
3. Column validation (41 columns)

## File Structure

```
src/
├── exctract/
│   ├── gamelog_data_acquisition.py              (Basketball Reference)
│   └── nba_api_gamelog_data_acquisition.py      (NBA API) ← NEW
├── transform/
│   ├── gamelog_cleaning_and_transformation.py   (Basketball Reference)
│   └── nba_api_gamelog_transformation.py        (NBA API) ← NEW
└── utils/
    └── logs.py

tests/
├── test_gamelog_data_acquisition.py
└── test_nba_api_gamelog_pipeline.py             ← NEW

pipeline_output/
├── nba_api_gamelog/                             ← NEW
│   └── gamelog_nba_api_{season}_all.csv
└── nba_api_transformed/                         ← NEW
    └── gamelog_nba_api_transformed_{season}.csv
```

## Column Mappings

### Input Schema (24 columns with _tm suffix)

After extraction from NBA API, the data has the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `id_season` | str | Season year (e.g., "2026") |
| `game_nb` | str | Game number in season |
| `game_date` | str | Date of game (YYYY-MM-DD) |
| `extdom` | str | Home/Away indicator ("" = home, "@" = away) |
| `tm` | str | Team abbreviation (e.g., "ATL") |
| `opp` | str | Opponent abbreviation |
| `results` | str | Win/Loss ("W" or "L") |
| `pts_tm` | str | Points scored by team |
| `fg_tm` | str | Field goals made |
| `fga_tm` | str | Field goals attempted |
| `fg_prct_tm` | str | Field goal percentage (e.g., ".422") |
| `3p_tm` | str | Three-pointers made |
| `3pa_tm` | str | Three-pointers attempted |
| `3p_prct_tm` | str | Three-point percentage |
| `ft_tm` | str | Free throws made |
| `fta_tm` | str | Free throws attempted |
| `ft_prct_tm` | str | Free throw percentage |
| `orb_tm` | str | Offensive rebounds |
| `trb_tm` | str | Total rebounds |
| `ast_tm` | str | Assists |
| `stl_tm` | str | Steals |
| `blk_tm` | str | Blocks |
| `tov_tm` | str | Turnovers |
| `pf_tm` | str | Personal fouls |

### Output Schema (41 columns)

After transformation, the data includes all input columns plus opponent statistics:

**Team Statistics (24 columns)**
- All columns from input with `_tm` suffix

**Opponent Statistics (17 columns)**
- `pts_opp`, `fg_opp`, `fga_opp`, `fg_prct_opp`
- `3p_opp`, `3pa_opp`, `3p_prct_opp`
- `ft_opp`, `fta_opp`, `ft_prct_opp`
- `orb_opp`, `trb_opp`, `ast_opp`
- `stl_opp`, `blk_opp`, `tov_opp`, `pf_opp`

## Configuration

### params.yaml

```yaml
# NBA API Pipeline Configuration
nba_api_gamelog_data_acquisition:
  team: all
  data_type: gamelog
  output_folder: pipeline_output/nba_api_gamelog/

nba_api_gamelog_transformation:
  output_folder: pipeline_output/nba_api_transformed/
  season_start_date: "2025-10-01"
  season_end_date: "2026-06-30"
```

### dvc.yaml

The pipeline includes three DVC stages:

1. **nba_api_gamelog_data_acquisition**
   - Extracts data from NBA API
   - Runs for each season in `global_params.season`
   - Dependencies: `nba_api_gamelog_data_acquisition.py`
   - Outputs: CSV in `nba_api_gamelog/`

2. **nba_api_gamelog_transformation**
   - Transforms raw data
   - Applies date filtering and self-merge
   - Dependencies: `nba_api_gamelog_transformation.py` + extraction output
   - Outputs: Transformed CSV in `nba_api_transformed/`

3. **nba_api_output_to_s3**
   - Uploads to S3 bucket
   - Dependencies: Transformation output
   - Destination: `s3://foreshadownba/data-engineering-pipeline-output/nba-api-gamelogs/`

## Running the Pipeline

### Run Complete NBA API Pipeline

```bash
dvc repro nba_api_output_to_s3
```

This will execute all three stages in order.

### Run Individual Stages

**Extraction only:**
```bash
dvc repro nba_api_gamelog_data_acquisition
```

**Transformation only:**
```bash
dvc repro nba_api_gamelog_transformation
```

**Upload only:**
```bash
dvc repro nba_api_output_to_s3
```

### Manual Execution

**Extraction:**
```bash
python3 -m src.exctract.nba_api_gamelog_data_acquisition \
  --data-type gamelog \
  --season 2026 \
  --team all \
  --output-folder pipeline_output/nba_api_gamelog/
```

**Transformation:**
```bash
python3 -m src.transform.nba_api_gamelog_transformation \
  --input-file pipeline_output/nba_api_gamelog/gamelog_nba_api_2026_all.csv \
  --output-folder pipeline_output/nba_api_transformed/ \
  --season 2026 \
  --season-start-date "2025-10-01" \
  --season-end-date "2026-06-30"
```

## Testing

### Run All Tests

```bash
python3 -m pytest tests/test_nba_api_gamelog_pipeline.py -v
```

### Run Specific Test

```bash
# Test ATL-TOR game validation
python3 -m pytest tests/test_nba_api_gamelog_pipeline.py::TestNBAAPIGamelogPipeline::test_03_transformation_logic_atl_tor_game -v

# Test column validation
python3 -m pytest tests/test_nba_api_gamelog_pipeline.py::TestNBAAPIGamelogPipeline::test_01_column_validation_input -v

# Test CSV output format
python3 -m pytest tests/test_nba_api_gamelog_pipeline.py::TestNBAAPIGamelogPipeline::test_04_csv_output_format -v
```

### Test Coverage

The test suite includes:

1. **Column Validation** - Input (24 columns) and output (41 columns) structure
2. **Transformation Logic** - Self-merge and opponent statistics creation
3. **ATL-TOR Game Validation** - Specific game data verification
4. **CSV Format Validation** - Exact output format matching expected CSV
5. **Date Filtering** - Season boundary filtering logic

## Example Output

### ATL-TOR Game on 2025-10-22 (Expected Output)

**CSV Format:**
```csv
2026,1,2025-10-22,,ATL,TOR,L,118,138,38,90,.422,10,35,.286,32,37,.865,8,34,25,7,6,16,24,54,95,.568,6,25,.240,24,29,.828,12,54,36,10,4,19,31
```

**Breakdown:**
- **Season:** 2026
- **Game Number:** 1
- **Date:** 2025-10-22
- **Location:** Home (empty extdom)
- **Matchup:** ATL vs TOR
- **Result:** Loss (L)
- **Score:** ATL 118, TOR 138
- **ATL Stats:** 38-90 FG (.422), 10-35 3P (.286), 32-37 FT (.865), 8 ORB, 34 TRB, 25 AST, 7 STL, 6 BLK, 16 TOV, 24 PF
- **TOR Stats:** 54-95 FG (.568), 6-25 3P (.240), 24-29 FT (.828), 12 ORB, 54 TRB, 36 AST, 10 STL, 4 BLK, 19 TOV, 31 PF

## Validation

The pipeline includes built-in validation at multiple stages:

### Extraction Validation

- Checks for expected 24 columns
- Logs data shape and column names
- Warns if any columns are missing

### Transformation Validation

- **Input Validation:** Verifies 24 input columns
- **Output Validation:** Verifies 41 output columns
- **Logging:** Detailed logs at each step
- **Error Handling:** Raises ValueError if validation fails

### Test Validation

- Unit tests verify column structure
- Integration tests verify transformation logic
- End-to-end tests verify complete pipeline

## Data Quality Notes

### Date Filtering

The transformation applies date filtering to ensure only games within the season boundaries are included:

- **Season Start:** October 1
- **Season End:** June 30
- **Format:** YYYY-MM-DD

Games outside this range are excluded from the output.

### Empty Values

- **extdom:** Empty string for home games, "@" for away games
- After transformation, empty strings are converted to `NaN`

### Data Types

All numeric fields are stored as strings to preserve formatting (e.g., ".422" instead of "0.422" for percentages).

## Troubleshooting

### Issue: No Data Returned from NBA API

**Possible Causes:**
- Season data not yet available
- Network/API connection issues
- Rate limiting

**Solution:**
- Verify season year is valid (past or current season)
- Check network connectivity
- Review API logs for error messages

### Issue: Column Validation Fails

**Possible Causes:**
- Input data structure changed
- Transformation logic error

**Solution:**
- Check input CSV has 24 columns with `_tm` suffix
- Verify transformation output has 41 columns
- Review error logs for specific missing columns

### Issue: Date Filtering Excludes All Games

**Possible Causes:**
- Incorrect season date range in params.yaml
- Game dates outside expected range

**Solution:**
- Verify `season_start_date` and `season_end_date` in params.yaml
- Check input data game dates
- Adjust date range if needed

## Future Enhancements

1. **Real-time Updates:** Add support for in-season data updates
2. **Team-specific Extraction:** Extract data for specific teams instead of "all"
3. **Data Type Conversion:** Convert string columns to appropriate numeric types
4. **Advanced Validation:** Add statistical validation (e.g., min/max values)
5. **Performance Optimization:** Parallel processing for multiple seasons

## Comparison: NBA API vs Basketball Reference

| Aspect | Basketball Reference | NBA API |
|--------|---------------------|---------|
| **Data Source** | Web scraping HTML | Direct API access |
| **Reliability** | Subject to website changes | More stable |
| **Speed** | Slower (HTML parsing) | Faster (JSON) |
| **Rate Limiting** | More restrictive | Less restrictive |
| **Data Format** | Requires parsing | Pre-formatted |
| **Maintenance** | Higher (website changes) | Lower |
| **Output Schema** | 24 columns (_tm) | 24 columns (_tm) |
| **Transformation** | Same logic | Same logic |

## Conclusion

The NBA API pipeline provides a more reliable and maintainable alternative to web scraping while maintaining full compatibility with existing transformation logic. The `basketball-reference-webscrapper` package's `WebScrapNBAApi` class ensures schema consistency, allowing seamless integration with the current pipeline.

## Support

For issues or questions:
- Review logs in the extraction/transformation scripts
- Run test suite to validate setup
- Check DVC pipeline status: `dvc status`
- Review this documentation

## Version History

- **v1.0.0** (2025-11-13): Initial NBA API pipeline implementation
  - Added NBA API extraction script
  - Added transformation script with validation
  - Created comprehensive test suite
  - Integrated with DVC pipeline
  - Created documentation
