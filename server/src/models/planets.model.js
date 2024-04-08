const fs = require('fs');
const path = require('path')
const { parse } = require('csv-parse');

const planets = require('./planets.mongo');

//validate the habitable planets
function isHabitablePlanet(planet) {
    const planetData = {
        name: planet['kepler_name'],
        disposition: planet['koi_disposition'],
        insolation: parseFloat(planet['koi_insol']),
        radius: parseFloat(planet['koi_prad']),
        temperature: parseFloat(planet['koi_steff']),
        distance: parseFloat(planet['koi_srad'])
    };

    return planetData['disposition'] === 'CONFIRMED' &&
        planetData['insolation'] > 0.36 && planetData['insolation'] < 1.11 &&
        planetData['radius'] < 1.6;
}


//sort the habitable planets from Radius
function sortPlanets(planets, sortBy) {
    return planets.sort((a, b) => {
        if(a[sortBy] < b[sortBy]) return -1;
        if(a[sortBy] > b[sortBy]) return 1;
        return 0; 
    })
}

function loadPlanetsData() {    
    return new Promise((resolve, reject) => {
            fs.createReadStream(path.join(__dirname, '..', '..', 'data', 'kepler_data.csv'))
            .pipe(parse({
                comment: '#',
                columns: true,
            })) 
            .on('data', async(data) => {
                if(isHabitablePlanet(data)) {
                    savePlanet(data);
                }
            })
            .on('error', (err) => {
                console.log(err);
                reject(err);
            })
            .on('end', async() => {
                const countPlanetsFound = (await getAllPlanets()).length;
                console.log(`${countPlanetsFound} habitable planets found!`);
                resolve();
            });
        });
}

async function getAllPlanets() {
    return await planets.find({});
}

async function savePlanet(planet) {
    try {
        await planets.updateOne({
            keplerName: planet.kepler_name,
        }, {
            keplerName: planet.kepler_name,
        }, {
            upsert: true,
        });
    } catch (err) {
        console.error(`Could not save planet: ${err}`);
    }
}

module.exports = {
    loadPlanetsData,
    getAllPlanets,
};