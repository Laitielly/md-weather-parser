print("=== Начинаем генерацию тестовых данных ===");

// Проверяем, существует ли база данных
const dbList = db.getMongo().getDBNames();
if (!dbList.includes("weather_db")) {
    print("Создаем базу данных weather_db...");
}

// Очищаем коллекции
print("Очищаем коллекции...");
db.current_weather.deleteMany({});
db.forecasts.deleteMany({});

print("Создаем тестовые данные о погоде...");

const now = new Date();
const cities = [
    { name: "Kaliningrad", country: "RU", lat: 54.7104, lon: 20.4522 }
];

// Генерируем данные за последние 3 дня
for (let day = 0; day < 5; day++) {
    for (let hour = 0; hour < 24; hour += 1) { // Каждые 3 часа
        const time = new Date(now);
        time.setDate(time.getDate() - day);
        time.setHours(time.getHours() - hour);
        
        for (const city of cities) {
            // Текущая погода
            const temp = 15 + Math.sin(day + hour/24) * 10 + (Math.random() * 5 - 2.5);
            const humidity = 60 + Math.random() * 30;
            const pressure = 1013 + Math.random() * 20 - 10;
            
            db.current_weather.insertOne({
                _id: `cw_${city.name.toLowerCase()}_${time.getTime()}`,
                city: city.name,
                country: city.country,
                dt: Math.floor(time.getTime() / 1000),
                temp: parseFloat(temp.toFixed(1)),
                feels_like: parseFloat((temp - Math.random() * 3).toFixed(1)),
                pressure: Math.round(pressure),
                humidity: Math.round(humidity),
                wind_speed: parseFloat((2 + Math.random() * 8).toFixed(1)),
                wind_deg: Math.floor(Math.random() * 360),
                weather_main: temp > 20 ? "Clear" : temp > 10 ? "Clouds" : "Rain",
                weather_description: temp > 20 ? "ясно" : temp > 10 ? "облачно" : "легкий дождь",
                clouds: Math.floor(Math.random() * 100),
                visibility: 10000 + Math.floor(Math.random() * 10000),
                collected_ts: time,
                created_at: new Date()
            });
            
            // Прогнозы на ближайшие 48 часов
            for (let forecastHour = 3; forecastHour <= 48; forecastHour += 3) {
                const forecastTime = new Date(time);
                forecastTime.setHours(forecastTime.getHours() + forecastHour);
                
                const forecastTemp = temp + (Math.random() * 4 - 2); // ±2 градуса от текущего
                const forecastError = Math.random() * 3 - 1.5; // Ошибка прогноза
                
                db.forecasts.insertOne({
                    _id: `fc_${city.name.toLowerCase()}_${time.getTime()}_${forecastHour}`,
                    city: city.name,
                    country: city.country,
                    forecast_dt: Math.floor(forecastTime.getTime() / 1000),
                    collection_dt: time,
                    temp: parseFloat((forecastTemp + forecastError).toFixed(1)),
                    feels_like: parseFloat((forecastTemp + forecastError - Math.random() * 3).toFixed(1)),
                    pressure: Math.round(pressure + Math.random() * 10 - 5),
                    humidity: Math.round(humidity + Math.random() * 20 - 10),
                    wind_speed: parseFloat((2 + Math.random() * 8).toFixed(1)),
                    wind_deg: Math.floor(Math.random() * 360),
                    weather_main: forecastTemp > 20 ? "Clear" : forecastTemp > 10 ? "Clouds" : "Rain",
                    weather_description: forecastTemp > 20 ? "ясно" : forecastTemp > 10 ? "облачно" : "легкий дождь",
                    clouds: Math.floor(Math.random() * 100),
                    pop: parseFloat(Math.min(0.8, Math.random() * 0.5).toFixed(2)), // Вероятность осадков
                    created_at: new Date()
                });
            }
        }
    }
}

// Создаем данные для проверки точности
print("Создаем данные для проверки точности прогнозов...");
const accuracyData = [];

for (let i = 0; i < 100; i++) {
    const forecastTime = new Date(now);
    forecastTime.setHours(forecastTime.getHours() - Math.floor(Math.random() * 48));
    
    const collectionTime = new Date(forecastTime);
    collectionTime.setHours(collectionTime.getHours() - Math.floor(Math.random() * 24));
    
    const verificationTime = new Date(forecastTime);
    verificationTime.setHours(verificationTime.getHours() + Math.floor(Math.random() * 3));
    
    const tempActual = 15 + Math.random() * 15;
    const tempError = Math.random() * 4 - 2; // Ошибка от -2 до +2 градусов
    
    accuracyData.push({
        forecast_dt: forecastTime,
        collection_dt: collectionTime,
        verification_dt: verificationTime,
        temp_actual: parseFloat(tempActual.toFixed(1)),
        temp_forecast: parseFloat((tempActual + tempError).toFixed(1)),
        temp_error: parseFloat(tempError.toFixed(1)),
        temp_absolute_error: parseFloat(Math.abs(tempError).toFixed(1)),
        humidity_actual: Math.floor(50 + Math.random() * 40),
        humidity_forecast: Math.floor(50 + Math.random() * 40),
        humidity_error: Math.floor(Math.random() * 20 - 10),
        pressure_actual: Math.floor(1000 + Math.random() * 30),
        pressure_forecast: Math.floor(1000 + Math.random() * 30),
        pressure_error: Math.floor(Math.random() * 10 - 5),
        weather_match: Math.random() > 0.3, // 70% совпадений
        created_at: new Date()
    });
}

// Статистика
const currentCount = db.current_weather.countDocuments();
const forecastCount = db.forecasts.countDocuments();
const accuracyCount = db.accuracy_metrics ? db.accuracy_metrics.countDocuments() : 0;

print("=== Генерация завершена ===");
print(`Текущая погода: ${currentCount} записей`);
print(`Прогнозы: ${forecastCount} записей`);
print(`Метрики точности: ${accuracyCount} записей`);
print("Пример данных текущей погоды:");
printjson(db.current_weather.findOne());