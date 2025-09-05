import React, { useState, useEffect } from 'react';
import { createClient } from '@supabase/supabase-js';

// Location mapping
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

const LOCATION_URLS = {
  '4182': '?store=4182',
  '1396': '?store=1396',
  '1257': '?store=1257', 
  '609': '?store=609',
  '1270': '?store=1270',
  '1002': '?store=1002',
  '1932': '?store=1932',
  '2911': '?store=2911'
};

// Default to State Street for demo, but this will be set via environment variable
const CURRENT_LOCATION = process.env.REACT_APP_LOCATION_ID || '4182';

// Supabase configuration
const supabaseUrl = process.env.REACT_APP_SUPABASE_URL || 'your-supabase-url';
const supabaseKey = process.env.REACT_APP_SUPABASE_ANON_KEY || 'your-supabase-key';
const supabase = createClient(supabaseUrl, supabaseKey);

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
      const today = getTodayPacific();
      const tomorrow = getTomorrowPacific();
      
      const { data, error } = await supabase
        .from('daily_appointments')
        .select('*')
        .eq('location_id', CURRENT_LOCATION)
        .in('appointment_date', [today, tomorrow])
        .order('appointment_time', { ascending: true });

      if (error) {
        console.error('Error fetching appointments:', error);
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
  }, []);

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
    
    // Collect all service fields
    if (appointment.service_type) services.push(appointment.service_type);
    if (appointment.service_type_2) services.push(appointment.service_type_2);
    if (appointment.service_type_3) services.push(appointment.service_type_3);
    
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
              {appointment.customer_name || 'Walk-in'}
            </span>
          </div>
          <div className="text-gray-700">
            {renderServices(appointment)}
          </div>
          {appointment.booking_status && (
            <div className={`inline-block px-2 py-1 rounded text-xs mt-2 ${
              appointment.booking_status.toLowerCase() === 'confirmed' 
                ? 'bg-green-100 text-green-800' 
                : 'bg-red-100 text-red-800'
            }`}>
              {appointment.booking_status}
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
                href={LOCATION_URLS[locationId]}
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
