#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "faker",
#     "fastparquet",
#     "pandas",
#     "pyarrow",
# ]
# ///
import uuid
import random
from datetime import datetime, timedelta
import json
import pandas as pd
from faker import Faker

fake = Faker('en_US')
Faker.seed(4321)

class DataGenerator:
    def __init__(self):
        self.customer_ids = [str(uuid.uuid4()) for _ in range(1000)]
        
    def generate_customer_profile(self, num_records=1000):
        """Génère des profils clients fictifs"""
        profiles = []
        
        for customer_id in self.customer_ids:
            profile = {
                'profile_id': customer_id,
                'email': fake.email(),
                'email_status': random.choice(['verified', 'pending', 'invalid']),
                'address': {
                    'street': fake.street_address(),
                    'city': fake.city(),
                    'country': 'France',
                    'geo': {
                        'lat': float(fake.latitude()),
                        'lon': float(fake.longitude())
                    }
                },
                'preferences': {
                    'communication': random.sample(
                        ['email', 'sms', 'postal'],
                        k=random.randint(1, 3)
                    )
                }
            }
            profiles.append(profile)
            
        return pd.DataFrame(profiles)
    
    def generate_customer_events(self, num_events=10000, start_date=None):
        """Génère des événements clients fictifs"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
            
        events = []
        event_types = ['login', 'logout', 'profile_update', 'consent_update']
        
        for _ in range(num_events):
            event_timestamp = fake.date_time_between(
                start_date=start_date,
                end_date='now'
            )
            
            event_type = random.choice(event_types)
            customer_id = random.choice(self.customer_ids)
            
            event = {
                'event_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'event_type': event_type,
                'event_timestamp': event_timestamp.isoformat(),
                'source_system': random.choice(['web', 'mobile', 'api']),
                'event_data': self._generate_event_data(event_type)
            }
            events.append(event)
            
        return pd.DataFrame(events)
    
    def _generate_event_data(self, event_type):
        """Génère des données spécifiques selon le type d'événement"""
        if event_type == 'login':
            return {
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'location': fake.city(),
                'success': random.random() > 0.1
            }
        elif event_type == 'profile_update':
            return {
                'updated_fields': random.sample(
                    ['email', 'address', 'preferences'],
                    k=random.randint(1, 3)
                ),
                'reason': random.choice(['user_request', 'system_update', 'verification'])
            }
        elif event_type == 'consent_update':
            return {
                'consent_type': random.choice(['marketing', 'analytics', 'communication']),
                'granted': random.random() > 0.3
            }
        else:
            return {}

def main():
    """Fonction principale pour générer et sauvegarder les données"""
    generator = DataGenerator()
    
    # Génération des profils
    profiles_df = generator.generate_customer_profile()
    profiles_df.to_parquet('data/bronze/customer_profiles.parquet')
    
    # Génération des événements
    events_df = generator.generate_customer_events()
    events_df.to_parquet('data/bronze/customer_events.parquet')
    
    # Quelques statistiques
    print("=== Données générées ===")
    print(f"Nombre de profils: {len(profiles_df)}")
    print(f"Nombre d'événements: {len(events_df)}")
    print("\nDistribution des statuts email:")
    print(profiles_df['email_status'].value_counts())
    print("\nDistribution des types d'événements:")
    print(events_df['event_type'].value_counts())

if __name__ == "__main__":
    main()
