/*
 * Weather Underground Personal Weather Station
 */

import groovy.transform.Field
import java.time.Instant

@Field static final String TEMPERATURE = 'temp'
@Field static final String HUMIDITY = 'humidity'
@Field static final String PRECIPITATION = 'precipTotal'

@Field static final String BOOL = 'bool'
@Field static final String NUMBER = 'number'
@Field static final String PASSWORD = 'password'
@Field static final String STRING = 'string'

@Field static final int DEFAULT_FREQUENCY = 60 * 60
@Field static final int DEFAULT_RETENTION = 24

public static final String version() { return '0.0.0' }

String averageAtt(String attribute) {
    return "${attribute}Avg"
}

metadata {
    definition(
        name: 'Weather Underground Personal Weather Station',
        namespace: 'hugoh',
        author: 'Hugo Haas',
        importUrl: 'https://github.com/hugoh/hubitat-wunderground-pws/blob/master/wunderground-pws.groovy'
    ) {
        capability 'Sensor'

        attribute HUMIDITY, NUMBER
        attribute PRECIPITATION, NUMBER
        attribute TEMPERATURE, NUMBER
        attribute averageAtt(TEMPERATURE), NUMBER

        command 'clearState', [[name:'Clears all state']]
    }
}

preferences {
    input name: 'weatherStation', type: STRING, title: 'Weather station to get observations from', required: true
    input name: 'apiKey', type: PASSWORD, title: 'Weather Underground API key', required: true
    input name: 'pollFrequency', type: NUMBER, title: 'Polling frequency in seconds', defaultValue: DEFAULT_FREQUENCY,
        required: true
    input name: 'hourSpan', type: NUMBER, title: 'Number of hours to calculate averages on',
        defaultValue: DEFAULT_RETENTION, required: true
    input name:'logEnable', type: BOOL, title: 'Enable debug logging', defaultValue: false
}

void initialize() {
    // Validate settings
    if (settings.weatherStation == null) {
        log.critical('Missing weather station')
        return
    }
    if (settings.pollFrequency < 0) {
        settings.pollFrequency = DEFAULT_FREQUENCY
    }
    if (settings.hourSpan < 0) {
        settings.hourSpan = DEFAULT_FREQUENCY
    }
    // Start loop
    log.info("Starting Weather Underground WPS device - version ${version()}")
    getWeatherObservationsLoop()
}

void updated() {
    log.info('Canceling any pending scheduled tasks')
    unschedule()
    initialize()
}

void clearAttributes() {
    log.info('Clearing all attribute values')
    for (String attribute: [HUMIDITY, PRECIPITATION, TEMPERATURE, averageAtt(TEMPERATURE)]) {
        device.deleteCurrentState(attribute)
    }
}

void clearState() {
    state.clear()
    clearAttributes()
}

Boolean checkStateVersion() {
    final int thisStateVersion = 1
    if (state.stateVersion == null) {
        log.debug("Initializing state version ${thisStateVersion}")
        clearState()
        state.stateVersion = thisStateVersion
        return true
    }
    if (state.stateVersion > thisStateVersion) {
        log.critical("""Incompatible state version ${state.stateVersion}.
Supported version by driver ${version()}: ${thisStateVersion}. 
Please either upgrade the device driver or clear the state.""") 
        throw new Exception("Incompatible version ${state.stateVersion}")
    }
    return true
}

@Field static final String OBS_PREFIX = 'obs'
@Field static final String OBS_DELIMITER = '-'
@Field static final Integer OBS_PREFIX_LEN = 4

String stateVariableName(long epoch) {
    return "${OBS_PREFIX}${OBS_DELIMITER}${epoch}"
}

void updateDeviceState(Map observation) {
    sendEvent(name: HUMIDITY, value: observation[HUMIDITY])
    sendEvent(name: PRECIPITATION, value: observation.imperial[PRECIPITATION])
    sendEvent(name: TEMPERATURE, value: observation.imperial[TEMPERATURE])
}

void addObservation(Map observation) {
    if (observation == null) {
        log.error('Cannot add blank observation')
        return
    }
    checkStateVersion()
    Instant instant = Instant.parse(observation.obsTimeUtc)
    long seconds = instant.getEpochSecond()
    String recordName = stateVariableName(seconds)
    state[recordName] = observation
    logDebug("Added value of record ${recordName}: ${observation}")
    updateDeviceState(observation)
}

List getAllObservations() {
    observationList = []
    for (String stateVar: state.keySet()) {
        if (!stateVar.startsWith("${OBS_PREFIX}${OBS_DELIMITER}")) {
            continue
        }
        observationList.add(stateVar)
    }
    logDebug("Found observations: ${observationList}")
    return observationList
}

void pruneOldObservations() {
    Long now = now() / 1000
    logDebug("Purging old data; epoch is now ${now}")
    Long delta = settings.hourSpan * 60 * 60
    for (String stateVar: getAllObservations()) {
        String timestamp = stateVar.substring(OBS_PREFIX_LEN)
        Long recordTimestamp = Long.parseLong(timestamp)
        Long secondsAgo = now - recordTimestamp
        if (secondsAgo > delta) {
            logDebug("Removing ${stateVar}: happened ${secondsAgo} seconds ago > ${delta}")
            state.remove(stateVar)
        } else {
            logDebug("Keeping ${stateVar}")
        }
    }
}

void computeAverages(observationList) {
    temperatureTotal = 1
    count = 0
    for (String stateVar: observationList) {
        temperatureTotal += state[stateVar].imperial.temp
        count++
    }
    temperatureAverage = temperatureTotal / count
    logDebug("Averaged ${count} observations: ${temperatureAverage}")
    sendEvent(name: averageAtt(TEMPERATURE), value: temperatureAverage)
}

Map getPwsObservation(String station, String apiKey) {
    final String wuApiUrl = 'https://api.weather.com/v2/pws/observations/current?format=json&units=e'
    String url = "${wuApiUrl}&stationId=${station}&apiKey=${apiKey}"
    Map ret = null
    logDebug("Getting weather data from ${url}")
    httpGet(url) { resp ->
        if (resp?.isSuccess()) {
            try {
                Map respJson = resp.getData()
                ret = respJson.observations[0]
            } catch (groovy.json.JsonException ex) {
                log.error("Could not get data from API: ${ex.getMessage()}")
            }
        } else {
            log.error("Could not get data from API: ${body}")
        }
    }
    return ret
}

void recordObservation() {
    Map observation = getPwsObservation(settings.weatherStation, settings.apiKey)
    if (observation == null) {
        log.error('Failed to record observation')
    } else {
        addObservation(observation)
    }
    pruneOldObservations()
    observationList = getAllObservations()
    if (observationList.size() == 0) {
        log.critical('No observation found -- that is a problem')
        clearAttributes()
    } else {
        computeAverages(observationList)
    }
}

void getWeatherObservationsLoop() {
    recordObservation()
    logDebug("Scheduling next check in ${settings.pollFrequency} seconds")
    runIn(settings.pollFrequency, 'recordObservation')
}

// --------------------------------------------------------------------------

void logDebug(String msg) {
    if (logEnable) {
        log.debug(msg)
    }
}
