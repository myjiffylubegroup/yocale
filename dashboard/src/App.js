import React, { useState, useEffect } from 'react';
import { createClient } from '@supabase/supabase-js';

// Location mapping - store numbers to display names
const LOCATIONS = {
  '4182': 'State Street',
  '1396': 'Santa Barbara', 
  '1257': 'Goleta',
  '609': 'Santa Maria',
  '1270': 'Arroyo Grande',
  '1002': 'San Luis Obispo',
  '1932': 'Atascadero',
  '2911': 'Paso Robles'
};

// Map store numbers to the actual location_business_id values in the database
const LOCATION_BUSINESS_IDS = {
  '4182': 212584,  // State Street
  '1396': 212583,  // Santa Barbara
  '1257': 212582,  // Goleta
  '609': 212581,   // Santa Maria
  '1270': 212580,  // Arroyo Grande
  '1002': 212579,  // San Luis Obispo
  '1932': 212578,  // Atascadero
  '2911': 212577   // Paso Robles
};

// Get location from URL parameter or default to State Street
const getLocationFromURL = () => {
  const urlParams = new URLSearchParams(window.location.search);
  return urlParams.get('store') || '4182';
};

const CURRENT_LOCATION = getLocationFromURL();

// Supabase configuration
const supabaseUrl = process.env.REACT_APP_SUPABASE_URL;
const supabaseKey = process.env.REACT_APP_SUPABASE_ANON_KEY;

// Only initialize Supabase if we have valid credentials
let supabase = null;
if (supabaseUrl && supabaseKey && supabaseUrl !== 'your-supabase-url') {
  supabase = createClient(supabaseUrl, supabaseKey);
}

const Dashboard = () => {
  const [appointments, setAppointments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [currentTime, setCurrentTime] = useState(new Date());

  // Get Pacific Time dates
  const getPacificDate = (date = new Date()) => {
    return new Date(date.toLocaleString("en-US", {timeZone: "America/Los_Angeles"}));
  };

  const getTodayPacific = () => {
    const today = getPacificDate();
    return today.toISOString().split('T')[0];
  };

  const getTomorrowPacific = () => {
    const tomorrow = getPacificDate();
    tomorrow.setDate(tomorrow.getDate() + 1);
    return tomorrow.toISOString().split('T')[0];
  };

  const formatTime = (timeString) => {
    if (!timeString) return 'No time';
    
    try {
      // Parse the time string and format in Pacific Time
      const date = new Date(timeString);
      return date.toLocaleString('en-US', {
        timeZone: 'America/Los_Angeles',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
      });
    } catch (error) {
      return timeString;
    }
  };

  const fetchAppointments = async () => {
    setLoading(true);
    try {
      if (!supabase) {
        console.error('Supabase not configured. Please set environment variables.');
        setAppointments([]);
        return;
      }

      const today = getTodayPacific();
      const tomorrow = getTomorrowPacific();
      
      const { data, error } = await supabase
        .from('daily_appointments')
        .select('*')
        .eq('location_business_id', LOCATION_BUSINESS_IDS[CURRENT_LOCATION])
        .in('appointment_date', [today, tomorrow])
        .order('appointment_time', { ascending: true });

      if (error) {
        console.error('Supabase error details:', error);
        console.error('Query parameters:', {
          location_business_id: LOCATION_BUSINESS_IDS[CURRENT_LOCATION],
          current_location: CURRENT_LOCATION,
          dates: [today, tomorrow]
        });
        return;
      }

      setAppointments(data || []);
    } catch (error) {
      console.error('Error fetching appointments:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAppointments();
    
    // Update current time every minute
    const timeInterval = setInterval(() => {
      setCurrentTime(new Date());
    }, 60000);

    // Refresh data every 5 minutes
    const dataInterval = setInterval(fetchAppointments, 5 * 60 * 1000);

    return () => {
      clearInterval(timeInterval);
      clearInterval(dataInterval);
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const filterAppointmentsByDate = (dateString) => {
    return appointments.filter(apt => {
      if (!apt.appointment_date) return false;
      const aptDate = new Date(apt.appointment_date).toISOString().split('T')[0];
      return aptDate === dateString;
    });
  };

  const todayAppointments = filterAppointmentsByDate(getTodayPacific());
  const tomorrowAppointments = filterAppointmentsByDate(getTomorrowPacific());

  const renderServices = (appointment) => {
    const services = [];
    
    // Collect all service fields based on actual table structure
    if (appointment.offering_name) services.push(appointment.offering_name);
    
    if (services.length === 0) return <span className="text-gray-500">No service listed</span>;
    
    return (
      <div>
        <div>{services[0]}</div>
        {services.slice(1).map((service, index) => (
          <div key={index} className="text-sm text-gray-600 ml-4">
            {service}
          </div>
        ))}
      </div>
    );
  };

  const AppointmentCard = ({ appointment }) => (
    <div className="bg-white rounded-lg shadow-md p-4 mb-3 border-l-4 border-blue-500">
      <div className="flex justify-between items-start">
        <div className="flex-1">
          <div className="flex items-center gap-4 mb-2">
            <span className="text-lg font-bold text-blue-600">
              {formatTime(appointment.appointment_time)}
            </span>
            <span className="text-lg font-semibold text-gray-800">
              {appointment.customer_name || `${appointment.client_first_name || ''} ${appointment.client_last_name || ''}`.trim() || 'Walk-in'}
            </span>
          </div>
          <div className="text-gray-700">
            {renderServices(appointment)}
          </div>
          {appointment.booking_status_label && (
            <div className={`inline-block px-2 py-1 rounded text-xs mt-2 ${
              appointment.booking_status_label.toLowerCase() === 'confirmed' 
                ? 'bg-green-100 text-green-800' 
                : 'bg-red-100 text-red-800'
            }`}>
              {appointment.booking_status_label}
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const AppointmentColumn = ({ title, appointments, isEmpty }) => (
    <div className="flex-1">
      <h2 className="text-2xl font-bold text-gray-800 mb-6 text-center">{title}</h2>
      <div className="space-y-3">
        {isEmpty ? (
          <div className="text-center text-gray-500 py-8">
            <div className="text-4xl mb-2">📅</div>
            <div>No appointments scheduled</div>
          </div>
        ) : (
          appointments.map((appointment, index) => (
            <AppointmentCard key={index} appointment={appointment} />
          ))
        )}
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      {/* Header */}
      <header className="bg-white shadow-lg">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                {LOCATIONS[CURRENT_LOCATION]} Jiffy Lube
              </h1>
              <p className="text-gray-600">Store #{CURRENT_LOCATION}</p>
            </div>
            <div className="text-right">
              <div className="text-lg font-semibold text-gray-800">
                {currentTime.toLocaleString('en-US', {
                  timeZone: 'America/Los_Angeles',
                  weekday: 'long',
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric'
                })}
              </div>
              <div className="text-sm text-gray-600">
                {currentTime.toLocaleString('en-US', {
                  timeZone: 'America/Los_Angeles',
                  hour: 'numeric',
                  minute: '2-digit',
                  hour12: true
                })} PT
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {loading ? (
          <div className="flex justify-center items-center py-20">
            <div className="text-xl text-gray-600">Loading appointments...</div>
          </div>
        ) : (
          <div className="flex gap-8">
            <AppointmentColumn 
              title="Today's Appointments"
              appointments={todayAppointments}
              isEmpty={todayAppointments.length === 0}
            />
            <AppointmentColumn 
              title="Tomorrow's Appointments" 
              appointments={tomorrowAppointments}
              isEmpty={tomorrowAppointments.length === 0}
            />
          </div>
        )}
      </main>

      {/* Location Navigation */}
      <footer className="bg-white border-t mt-12">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex flex-wrap justify-center gap-2">
            {Object.entries(LOCATIONS).map(([locationId, locationName]) => (
              <a
                key={locationId}
                href={`${window.location.pathname}?store=${locationId}`}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  locationId === CURRENT_LOCATION
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {locationName}
              </a>
            ))}
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Dashboard;
