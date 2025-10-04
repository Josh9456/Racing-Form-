import requests
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import time

class LadbrokesRacingScraper:
    """
    Enhanced scraper for Ladbrokes Racing API with full support for international races.
    Includes comprehensive form data retrieval for all regions including New Zealand, 
    Hong Kong, Japan, Korea, and European races.
    """

    def __init__(self, email, partner_name, base_dir="racing_data"):
        """
        Initialize the scraper with API credentials and base directory.
        
        Args:
            email: Your email for the 'From' header
            partner_name: Your partner name for the 'X-Partner' header
            base_dir: Base directory for storing race data (default: "racing_data")
        """
        self.base_url = "https://api-affiliates.ladbrokes.com.au/affiliates/v1/racing"
        self.headers = {
            "From": email,
            "X-Partner": partner_name
        }
        
        self.base_dir = base_dir
        # Enhanced country codes for international racing
        self.country_codes = {
            "AUS": "Australia",
            "NZL": "New Zealand",
            "HKG": "Hong Kong",
            "SGP": "Singapore",
            "JPN": "Japan",
            "KOR": "South Korea",
            "GBR": "Great Britain",
            "IRL": "Ireland",
            "USA": "United States",
            "CAN": "Canada",
            "FRA": "France",
            "GER": "Germany",
            "ITA": "Italy",
            "ESP": "Spain",
            "ARG": "Argentina",
            "ZAF": "South Africa",
            "UAE": "United Arab Emirates",
            "SAU": "Saudi Arabia",
            "CHI": "Chile",
            "BRA": "Brazil",
            "PER": "Peru",
            "MEX": "Mexico",
            "MAC": "Macau",
            "MAL": "Malaysia",
            "IND": "India"
        }

    def get_meetings(self, date=None, categories=None, countries=None):
        """
        Fetch all racing meetings for a specific date and countries.
        
        Args:
            date: Date string in YYYY-MM-DD format (default: today)
            categories: List of categories ['T', 'H', 'G'] (default: ['T', 'G'])
            countries: List of country codes (default: ['AUS'])
        
        Returns:
            List of meeting dictionaries
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        if categories is None:
            categories = ['T', 'G']  # Thoroughbred and Greyhound
        if countries is None:
            countries = ['AUS']
        
        all_meetings = []
        
        for country in countries:
            for category in categories:
                url = f"{self.base_url}/meetings"
                params = {
                    "enc": "json",
                    "date_from": date,
                    "date_to": date,
                    "category": category,
                    "country": country,
                    "limit": 200
                }
                
                try:
                    response = requests.get(url, headers=self.headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    if "data" in data and "meetings" in data["data"]:
                        meetings = data["data"]["meetings"]
                        country_name = self.country_codes.get(country, country)
                        if meetings:
                            print(f"Found {len(meetings)} {category} meetings in {country_name}")
                        all_meetings.extend(meetings)
                    
                    time.sleep(0.5)  # Be polite to the API
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching {category} meetings for {country}: {e}")
        
        return all_meetings

    def get_race_details(self, race_id, country=None):
        """
        Fetch detailed information for a specific race with enhanced form data.
        Now includes comprehensive form retrieval for international races.
        
        Args:
            race_id: The unique race ID
            country: Country code for the race (helps optimize data retrieval)
        
        Returns:
            Dictionary containing comprehensive race details including form data
        """
        url = f"{self.base_url}/events/{race_id}"
        
        # Enhanced parameters to ensure form data is included for all regions
        params = {
            "enc": "json",
            "include_form": "true",  # Explicitly request form data
            "include_odds": "true",  # Include odds history
            "include_flucs": "true",  # Include fluctuations
            "include_speedmap": "true",  # Include speed maps
            "include_past_performances": "true",  # Include past performances
            "include_form_indicators": "true",  # Include form indicators
            "include_jockey_stats": "true",  # Include jockey statistics
            "include_trainer_stats": "true"  # Include trainer statistics
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            race_data = response.json()
            
            # If initial request doesn't have comprehensive form data, try alternative endpoint
            if self._is_form_data_incomplete(race_data, country):
                print(f"   → Fetching enhanced form data for international race...")
                race_data = self._fetch_enhanced_form_data(race_id, race_data, country)
            
            return race_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching race {race_id}: {e}")
            return None

    def _is_form_data_incomplete(self, race_data, country):
        """
        Check if the race data contains incomplete form information.
        International races sometimes need additional requests.
        
        Args:
            race_data: The race data dictionary
            country: Country code
        
        Returns:
            Boolean indicating if form data needs enhancement
        """
        if not race_data or "data" not in race_data:
            return False
        
        data = race_data.get("data", {})
        runners = data.get("runners", [])
        
        if not runners:
            return False
        
        # Check for international countries that often need enhanced data
        international_countries = ['NZL', 'HKG', 'JPN', 'KOR', 'GBR', 'IRL', 'FRA', 
                                   'GER', 'ITA', 'ESP', 'SGP', 'MAC', 'CHI', 'ARG']
        
        race_country = data.get("race", {}).get("country", "")
        is_international = race_country in international_countries or country in international_countries
        
        # Check if runners have comprehensive form data
        for runner in runners[:3]:  # Check first 3 runners as sample
            has_form_comment = bool(runner.get("form_comment"))
            has_last_starts = bool(runner.get("last_twenty_starts"))
            has_past_performances = bool(runner.get("past_performances"))
            has_form_indicators = bool(runner.get("form_indicators"))
            
            # If international race and missing key form fields, needs enhancement
            if is_international and not (has_form_comment or has_last_starts or 
                                        has_past_performances or has_form_indicators):
                return True
        
        return False

    def _fetch_enhanced_form_data(self, race_id, initial_data, country):
        """
        Fetch enhanced form data using alternative methods for international races.
        
        Args:
            race_id: The unique race ID
            initial_data: Initial race data that may be incomplete
            country: Country code
        
        Returns:
            Enhanced race data dictionary
        """
        enhanced_data = initial_data.copy() if initial_data else {}
        
        try:
            # Method 1: Try form-specific endpoint
            form_url = f"{self.base_url}/events/{race_id}/form"
            form_params = {"enc": "json"}
            
            form_response = requests.get(form_url, headers=self.headers, 
                                        params=form_params, timeout=30)
            
            if form_response.status_code == 200:
                form_data = form_response.json()
                enhanced_data = self._merge_form_data(enhanced_data, form_data)
                print(f"   ✓ Enhanced form data retrieved")
            
            time.sleep(0.3)
            
            # Method 2: Try runner-specific details
            if "data" in enhanced_data and "runners" in enhanced_data["data"]:
                runners = enhanced_data["data"]["runners"]
                for idx, runner in enumerate(runners):
                    runner_id = runner.get("entrant_id") or runner.get("competitor_id")
                    if runner_id:
                        runner_details = self._fetch_runner_details(runner_id, race_id)
                        if runner_details:
                            enhanced_data["data"]["runners"][idx] = self._merge_runner_data(
                                runner, runner_details
                            )
                        
                        # Rate limit
                        if idx < len(runners) - 1:
                            time.sleep(0.2)
            
        except Exception as e:
            print(f"   ⚠ Could not fetch enhanced form data: {e}")
        
        return enhanced_data

    def _fetch_runner_details(self, runner_id, race_id):
        """
        Fetch detailed information for a specific runner.
        
        Args:
            runner_id: The runner/entrant ID
            race_id: The race ID
        
        Returns:
            Dictionary containing runner details
        """
        try:
            url = f"{self.base_url}/runners/{runner_id}"
            params = {
                "enc": "json",
                "race_id": race_id,
                "include_form": "true"
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=20)
            
            if response.status_code == 200:
                return response.json()
            
        except:
            pass
        
        return None

    def _merge_form_data(self, base_data, form_data):
        """
        Merge form-specific data into base race data.
        
        Args:
            base_data: Base race data dictionary
            form_data: Form-specific data dictionary
        
        Returns:
            Merged data dictionary
        """
        if not form_data or "data" not in form_data:
            return base_data
        
        if "data" not in base_data:
            base_data["data"] = {}
        
        # Merge runner form data
        if "runners" in form_data["data"]:
            form_runners = {r.get("entrant_id") or r.get("runner_number"): r 
                           for r in form_data["data"]["runners"]}
            
            if "runners" in base_data["data"]:
                for idx, runner in enumerate(base_data["data"]["runners"]):
                    runner_id = runner.get("entrant_id") or runner.get("runner_number")
                    if runner_id in form_runners:
                        base_data["data"]["runners"][idx] = self._merge_runner_data(
                            runner, {"data": form_runners[runner_id]}
                        )
        
        return base_data

    def _merge_runner_data(self, base_runner, detail_data):
        """
        Merge detailed runner data into base runner data.
        
        Args:
            base_runner: Base runner dictionary
            detail_data: Detailed runner data
        
        Returns:
            Merged runner dictionary
        """
        if not detail_data or "data" not in detail_data:
            return base_runner
        
        detail_runner = detail_data["data"]
        
        # Fields to merge if missing in base
        merge_fields = [
            "form_comment", "last_twenty_starts", "past_performances",
            "form_indicators", "best_time", "speedmap", "jockey_past_performances",
            "trainer_statistics", "gear", "flucs_with_timestamp", "class_level",
            "recent_form", "track_stats", "distance_stats"
        ]
        
        for field in merge_fields:
            if field in detail_runner and (field not in base_runner or not base_runner[field]):
                base_runner[field] = detail_runner[field]
        
        return base_runner

    def sanitize_filename(self, name):
        """
        Sanitize a string to be used as a filename.
        
        Args:
            name: String to sanitize
        
        Returns:
            Sanitized string safe for use as filename
        """
        # Remove or replace invalid filename characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        return name.strip()

    def prompt_for_countries(self):
        """
        Interactively prompt user for countries to include.
        
        Returns:
            List of country codes selected by user
        """
        print("\n" + "="*70)
        print("SELECT COUNTRIES TO SCRAPE")
        print("="*70)
        print("\nAvailable countries:")
        print("-" * 70)
        
        # Display countries in a nice format
        sorted_countries = sorted(self.country_codes.items(), key=lambda x: x[1])
        for idx, (code, name) in enumerate(sorted_countries, 1):
            print(f"{idx:2}. {code:3} - {name}")
        
        print("\n" + "-" * 70)
        print("\nOptions:")
        print(" • Enter country codes separated by commas (e.g., AUS,HKG,NZL)")
        print(" • Enter numbers separated by commas (e.g., 1,3,4)")
        print(" • Press ENTER for Australia only (default)")
        print(" • Type 'ALL' for all countries")
        
        user_input = input("\nYour selection: ").strip().upper()
        
        if not user_input:
            print("\n✓ Selected: Australia only")
            return ['AUS']
        
        if user_input == 'ALL':
            print(f"\n✓ Selected: All {len(self.country_codes)} countries")
            return list(self.country_codes.keys())
        
        # Parse input
        selected_countries = []
        inputs = [x.strip() for x in user_input.split(',')]
        
        for inp in inputs:
            # Check if it's a number
            if inp.isdigit():
                idx = int(inp) - 1
                if 0 <= idx < len(sorted_countries):
                    selected_countries.append(sorted_countries[idx][0])
            # Check if it's a valid country code
            elif inp in self.country_codes:
                selected_countries.append(inp)
        
        if not selected_countries:
            print("\n⚠ No valid countries selected. Using Australia as default.")
            return ['AUS']
        
        # Display selected countries
        print("\n✓ Selected countries:")
        for code in selected_countries:
            print(f" • {code} - {self.country_codes.get(code, code)}")
        
        return selected_countries

    def prompt_for_categories(self):
        """
        Interactively prompt user for race categories.
        
        Returns:
            List of category codes selected by user
        """
        print("\n" + "="*70)
        print("SELECT RACE CATEGORIES")
        print("="*70)
        print("\n1. T - Thoroughbred (Horses)")
        print("2. G - Greyhound (Dogs)")
        print("3. H - Harness (Trotters/Pacers)")
        print("\nOptions:")
        print(" • Enter category codes separated by commas (e.g., T,G)")
        print(" • Enter numbers separated by commas (e.g., 1,2)")
        print(" • Press ENTER for Thoroughbred & Greyhound (default)")
        print(" • Type 'ALL' for all categories")
        
        user_input = input("\nYour selection: ").strip().upper()
        
        if not user_input:
            print("\n✓ Selected: Thoroughbred & Greyhound")
            return ['T', 'G']
        
        if user_input == 'ALL':
            print("\n✓ Selected: All categories")
            return ['T', 'G', 'H']
        
        # Parse input
        category_map = {'1': 'T', '2': 'G', '3': 'H', 'T': 'T', 'G': 'G', 'H': 'H'}
        selected_categories = []
        inputs = [x.strip() for x in user_input.split(',')]
        
        for inp in inputs:
            if inp in category_map:
                cat = category_map[inp]
                if cat not in selected_categories:
                    selected_categories.append(cat)
        
        if not selected_categories:
            print("\n⚠ No valid categories selected. Using T & G as default.")
            return ['T', 'G']
        
        # Display selected categories
        cat_names = {'T': 'Thoroughbred', 'G': 'Greyhound', 'H': 'Harness'}
        print("\n✓ Selected categories:")
        for cat in selected_categories:
            print(f" • {cat} - {cat_names[cat]}")
        
        return selected_categories

    def scrape_and_save(self, date=None, interactive=True, countries=None, categories=None):
        """
        Main method to scrape all meetings and races, organizing into folders.
        
        Args:
            date: Date string in YYYY-MM-DD format (default: today)
            interactive: If True, prompt user for countries and categories
            countries: List of country codes (used if interactive=False)
            categories: List of category codes (used if interactive=False)
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        # Interactive mode
        if interactive:
            categories = self.prompt_for_categories()
            countries = self.prompt_for_countries()
        else:
            if countries is None:
                countries = ['AUS']
            if categories is None:
                categories = ['T', 'G']
        
        print(f"\n{'='*70}")
        print(f"STARTING SCRAPE FOR {date}")
        print(f"{'='*70}\n")
        
        # Create base directory structure
        date_dir = Path(self.base_dir) / date
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Fetch all meetings
        print("Fetching meetings...")
        print("-" * 70)
        meetings = self.get_meetings(date=date, categories=categories, countries=countries)
        
        if not meetings:
            print("\n⚠ No meetings found for this date and selection.")
            return
        
        print(f"\n{'='*70}")
        print(f"FOUND {len(meetings)} TOTAL MEETINGS")
        print(f"{'='*70}\n")
        
        # Process each meeting
        for idx, meeting in enumerate(meetings, 1):
            meeting_name = meeting.get("name", "Unknown")
            meeting_id = meeting.get("meeting", "")
            category_name = meeting.get("category_name", "Unknown")
            country = meeting.get("country", "")
            state = meeting.get("state", "")
            
            location = f"{country}"
            if state:
                location += f", {state}"
            
            print(f"[{idx}/{len(meetings)}] Processing: {meeting_name} ({category_name}, {location})")
            
            # Create meeting folder with country code
            folder_name = self.sanitize_filename(
                f"{meeting_name}_{category_name.split()[0]}_{country}_{state}".replace("__", "_").rstrip("_")
            )
            meeting_dir = date_dir / folder_name
            meeting_dir.mkdir(exist_ok=True)
            
            # Save meeting overview
            meeting_overview_path = meeting_dir / "meeting_info.json"
            with open(meeting_overview_path, 'w', encoding='utf-8') as f:
                json.dump(meeting, f, indent=2, ensure_ascii=False)
            
            # Process each race in the meeting
            races = meeting.get("races", [])
            print(f"   Found {len(races)} races")
            
            for race in races:
                race_id = race.get("id", "")
                race_number = race.get("race_number", 0)
                race_name = race.get("name", "Unknown Race")
                
                if not race_id:
                    continue
                
                print(f"   Fetching Race {race_number}: {race_name}...")
                
                # Fetch detailed race information with enhanced form data
                race_details = self.get_race_details(race_id, country=country)
                
                if race_details:
                    # Save race details to file
                    race_filename = self.sanitize_filename(
                        f"Race_{race_number:02d}_{race_name}.json"
                    )
                    race_path = meeting_dir / race_filename
                    
                    with open(race_path, 'w', encoding='utf-8') as f:
                        json.dump(race_details, f, indent=2, ensure_ascii=False)
                    
                    print(f"   ✓ Saved to {race_filename}")
                else:
                    print(f"   ✗ Failed to fetch race details")
                
                time.sleep(0.5)  # Be polite to the API
            
            print()  # Blank line between meetings
        
        print(f"{'='*70}")
        print(f"SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print(f"Data saved to: {date_dir}\n")
        
        # Generate summary
        self.generate_summary(date_dir)

    def generate_summary(self, date_dir):
        """
        Generate a summary file of all meetings and races.
        
        Args:
            date_dir: Path to the date directory
        """
        summary = {
            "date": date_dir.name,
            "total_meetings": 0,
            "total_races": 0,
            "meetings": [],
            "countries": {}
        }
        
        for meeting_dir in sorted(date_dir.iterdir()):
            if meeting_dir.is_dir():
                summary["total_meetings"] += 1
                
                # Count race files
                race_files = list(meeting_dir.glob("Race_*.json"))
                summary["total_races"] += len(race_files)
                
                # Extract country from folder name
                parts = meeting_dir.name.split('_')
                country = parts[-2] if len(parts) >= 2 else "Unknown"
                
                if country not in summary["countries"]:
                    summary["countries"][country] = 0
                summary["countries"][country] += 1
                
                summary["meetings"].append({
                    "folder": meeting_dir.name,
                    "race_count": len(race_files),
                    "country": country
                })
        
        summary_path = date_dir / "summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"Total Meetings: {summary['total_meetings']}")
        print(f"Total Races: {summary['total_races']}")
        print(f"\nBy Country:")
        for country, count in sorted(summary['countries'].items()):
            print(f" • {country}: {count} meetings")
        print(f"{'='*70}\n")


def main():
    """
    Main function to run the scraper with interactive mode.
    Update the email and partner_name with your credentials.
    """
    # ===== CONFIGURE YOUR CREDENTIALS HERE =====
    EMAIL = "your.email@example.com"  # Replace with your email
    PARTNER_NAME = "Your Partner Name"  # Replace with your partner name
    # ============================================
    
    # Create scraper instance
    scraper = LadbrokesRacingScraper(
        email=EMAIL,
        partner_name=PARTNER_NAME,
        base_dir="racing_data"
    )
    
    print("\n" + "="*70)
    print("LADBROKES RACING DATA SCRAPER - ENHANCED INTERNATIONAL SUPPORT")
    print("="*70)
    print("\nFeatures:")
    print(" • Full form data for ALL international races")
    print(" • Comprehensive support for NZ, HK, JP, KR, EU races")
    print(" • Enhanced runner details and past performances")
    print(" • Automatic form data enhancement for international races")
    print("="*70)
    
    # Scrape today's races with interactive prompts
    scraper.scrape_and_save(interactive=True)
    
    # Non-interactive examples (uncomment to use):
    # scraper.scrape_and_save(interactive=False, countries=['AUS'], categories=['T', 'G'])
    # scraper.scrape_and_save(interactive=False, countries=['AUS', 'NZL', 'HKG'], categories=['T'])
    # scraper.scrape_and_save(date="2025-10-04", interactive=False, countries=['NZL', 'HKG', 'JPN'])


if __name__ == "__main__":
    main()
