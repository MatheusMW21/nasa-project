const { parse } = require('csv-parse');
const fs = require('fs');

const habitablePlanets = [];

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

fs.createReadStream('kepler_data.csv')
    .pipe(parse({
        comment: '#',
        columns: true,
    })) 
    .on('data', (data) => {
        if(isHabitablePlanet(data)) {
            habitablePlanets.push(data);
        }
    })
    .on('error', (err) => {
        console.log(err);
    })
    .on('end', () => {
        console.log(`${habitablePlanets.length} habitable planets found!`);
        
        if (habitablePlanets.length > 0) {
            const sortedByRadius = sortPlanets(habitablePlanets, 'koi_prad');

            console.log("Habitable planets sorted by radius:");
            sortedByRadius.forEach(planet => {
                console.log(`${planet['kepler_name']} - Radius: ${planet['koi_prad']}`);
            });
        } else {
            console.log("No habitable planets found for sorting.");
        }
    });

module.exports = {
    planets: habitablePlanets,
};