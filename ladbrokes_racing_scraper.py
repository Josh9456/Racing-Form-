
import requests
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import time

class LadbrokesRacingScraper:
    """
    Scraper for Ladbrokes Racing API to fetch Australian horse and greyhound racing data.
    Organizes data by meeting and race into a structured folder system.
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

    def get_meetings(self, date=None, categories=None, country="AUS"):
        """
        Fetch all racing meetings for a specific date.

        Args:
            date: Date string in YYYY-MM-DD format (default: today)
            categories: List of categories ['T', 'H', 'G'] (default: ['T', 'G'])
            country: Country code (default: "AUS")

        Returns:
            List of meeting dictionaries
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        if categories is None:
            categories = ['T', 'G']  # Thoroughbred and Greyhound

        all_meetings = []

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
                    print(f"Found {len(meetings)} {category} meetings")
                    all_meetings.extend(meetings)

                time.sleep(0.5)  # Be polite to the API

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {category} meetings: {e}")

        return all_meetings

    def get_race_details(self, race_id):
        """
        Fetch detailed information for a specific race.

        Args:
            race_id: The unique race ID

        Returns:
            Dictionary containing race details
        """
        url = f"{self.base_url}/events/{race_id}"
        params = {"enc": "json"}

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching race {race_id}: {e}")
            return None

    def sanitize_filename(self, name):
        """
        Sanitize a string to be used as a filename.

        Args:
            name: String to sanitize

        Returns:
            Sanitized string safe for use as filename
        """
        # Remove or replace invalid filename characters
        invalid_chars = '<>:"/\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        return name.strip()

    def scrape_and_save(self, date=None):
        """
        Main method to scrape all meetings and races, organizing into folders.

        Args:
            date: Date string in YYYY-MM-DD format (default: today)
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        print(f"\n{'='*60}")
        print(f"Starting scrape for {date}")
        print(f"{'='*60}\n")

        # Create base directory structure
        date_dir = Path(self.base_dir) / date
        date_dir.mkdir(parents=True, exist_ok=True)

        # Fetch all meetings
        print("Fetching meetings...")
        meetings = self.get_meetings(date=date)

        if not meetings:
            print("No meetings found for this date.")
            return

        print(f"\nFound {len(meetings)} total meetings\n")

        # Process each meeting
        for idx, meeting in enumerate(meetings, 1):
            meeting_name = meeting.get("name", "Unknown")
            meeting_id = meeting.get("meeting", "")
            category_name = meeting.get("category_name", "Unknown")
            state = meeting.get("state", "")

            print(f"[{idx}/{len(meetings)}] Processing: {meeting_name} ({category_name}, {state})")

            # Create meeting folder
            folder_name = self.sanitize_filename(
                f"{meeting_name}_{category_name.split()[0]}_{state}"
            )
            meeting_dir = date_dir / folder_name
            meeting_dir.mkdir(exist_ok=True)

            # Save meeting overview
            meeting_overview_path = meeting_dir / "meeting_info.json"
            with open(meeting_overview_path, 'w', encoding='utf-8') as f:
                json.dump(meeting, f, indent=2, ensure_ascii=False)

            # Process each race in the meeting
            races = meeting.get("races", [])
            print(f"  Found {len(races)} races")

            for race in races:
                race_id = race.get("id", "")
                race_number = race.get("race_number", 0)
                race_name = race.get("name", "Unknown Race")

                if not race_id:
                    continue

                print(f"    Fetching Race {race_number}: {race_name}...")

                # Fetch detailed race information
                race_details = self.get_race_details(race_id)

                if race_details:
                    # Save race details to file
                    race_filename = self.sanitize_filename(
                        f"Race_{race_number:02d}_{race_name}.json"
                    )
                    race_path = meeting_dir / race_filename

                    with open(race_path, 'w', encoding='utf-8') as f:
                        json.dump(race_details, f, indent=2, ensure_ascii=False)

                    print(f"    ✓ Saved to {race_filename}")
                else:
                    print(f"    ✗ Failed to fetch race details")

                time.sleep(0.5)  # Be polite to the API

            print()  # Blank line between meetings

        print(f"{'='*60}")
        print(f"Scraping complete! Data saved to: {date_dir}")
        print(f"{'='*60}\n")

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
            "meetings": []
        }

        for meeting_dir in sorted(date_dir.iterdir()):
            if meeting_dir.is_dir():
                summary["total_meetings"] += 1

                # Count race files
                race_files = list(meeting_dir.glob("Race_*.json"))
                summary["total_races"] += len(race_files)

                summary["meetings"].append({
                    "folder": meeting_dir.name,
                    "race_count": len(race_files)
                })

        summary_path = date_dir / "summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        print(f"Summary: {summary['total_meetings']} meetings, {summary['total_races']} races")


def main():
    """
    Main function to run the scraper.
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

    # Scrape today's races (or specify a date: "2025-10-03")
    scraper.scrape_and_save()

    # Optional: You can also scrape specific dates
    # scraper.scrape_and_save(date="2025-10-04")


if __name__ == "__main__":
    main()
