#!/usr/bin/env python3
"""
Data generation module for benchmarks with different scales
"""
import uuid
import random
from datetime import datetime, timedelta
import json
import pandas as pd
from faker import Faker
from pathlib import Path
import os

fake = Faker('en_US')
Faker.seed(4321)

class DataGenerator:
    def __init__(self, num_profiles):
        self.num_profiles = num_profiles
        self.customer_ids = [str(uuid.uuid4()) for _ in range(num_profiles)]
        
    def generate_customer_profile(self):
        """Generate customer profiles"""
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
    
    def generate_customer_events(self, num_events, start_date=None):
        """Generate customer events"""
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
        """Generate event-specific data"""
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

def generate_data_if_needed(scale_name, num_profiles, num_events):
    """Generate data for a specific scale if it doesn't exist"""
    base_path = Path(__file__).parent.parent
    data_dir = base_path / 'data' / scale_name
    profiles_path = data_dir / 'customer_profiles.parquet'
    events_path = data_dir / 'customer_events.parquet'
    
    # Check if data already exists
    if profiles_path.exists() and events_path.exists():
        print(f"✓ Data for scale '{scale_name}' already exists")
        return profiles_path, events_path
    
    print(f"🔄 Generating data for scale '{scale_name}' ({num_profiles:,} profiles, {num_events:,} events)...")
    
    # Create directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    generator = DataGenerator(num_profiles)
    
    # Generate profiles
    profiles_df = generator.generate_customer_profile()
    profiles_df.to_parquet(profiles_path)
    
    # Generate events
    events_df = generator.generate_customer_events(num_events)
    events_df.to_parquet(events_path)
    
    print(f"✅ Generated {len(profiles_df):,} profiles and {len(events_df):,} events for '{scale_name}'")
    
    return profiles_path, events_path

# Define benchmark scales
BENCHMARK_SCALES = {
    'small': {'profiles': 10000, 'events': 100000},
    'medium': {'profiles': 100000, 'events': 1000000}, 
    'large': {'profiles': 100000, 'events': 10000000},
    'xlarge': {'profiles': 1000000, 'events': 100000000}
}

def get_data_paths_for_scale(scale_name):
    """Get data paths for a specific scale"""
    if scale_name not in BENCHMARK_SCALES:
        raise ValueError(f"Unknown scale: {scale_name}. Available: {list(BENCHMARK_SCALES.keys())}")
    
    scale_config = BENCHMARK_SCALES[scale_name]
    profiles_path, events_path = generate_data_if_needed(
        scale_name, 
        scale_config['profiles'], 
        scale_config['events']
    )
    
    return {
        'profiles': profiles_path,
        'events': events_path
    }