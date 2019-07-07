const inputName = document.getElementById('name');
const form = document.getElementById('search-form');
const map = document.getElementById('map');

const restrictions = {'country': ['us']};

/* eslint-disable no-undef */
autocomplete = new google.maps.places.Autocomplete(inputName);
autocomplete.setFields(['geometry']);
autocomplete.setComponentRestrictions(restrictions);

const acService = new google.maps.places.AutocompleteService();
const placesService = new google.maps.places.PlacesService(map);
const sessionToken = new google.maps.places.AutocompleteSessionToken();
/* eslint-enable no-undef */

function showResults(response) {
    const results = document.getElementById('results');
    results.innerHTML = response;

    for (let indicator of document.getElementsByClassName('indicator')) {
        const graph = indicator.querySelector('.graph');
        if (!graph) {
            console.error(`Could not find a graph element for ${indicator.id}`);
            continue;
        }

        const trace = {
            x: JSON.parse(graph.dataset.x),
            y: JSON.parse(graph.dataset.y),
            mode: 'lines+markers',
            type: 'scatter'
        };

        const layout = {
            xaxis: {'title': 'year'},
            yaxis: {'title': graph.dataset.units}
        };

        Plotly.newPlot(graph, [trace], layout, {responsive: true}); // eslint-disable-line no-undef
    }
}

function setURL(location) {
    location = location.toJSON();

    // Truncate to 6 decimal places
    for (const key in location) {
        location[key] = location[key].toFixed(6);
    }

    const params = new URLSearchParams(location);
    history.pushState(null, null, `/?${params}`);

    // Clear search box
    inputName.value = '';
}

function setLocation(location) {
    const formData = new FormData(form);
    formData.set('location', JSON.stringify(location));

    fetch(form.getAttribute('action'), {
        method: 'POST',
        body: formData
    })
        .then(response => response.text())
        .then(showResults)
        .then(() => setURL(location))
        .catch(error => console.log('Error submitting form: ', error));
}

function getTopLocation(predictions, status) {
    // eslint-disable-next-line no-undef
    if (status !== google.maps.places.PlacesServiceStatus.OK) {
        console.error('Could not get predictions: status is ' + status);
        return;
    }

    const request = {
        fields: ['geometry.location'],
        placeId: predictions[0].place_id,
        sessionToken: sessionToken
    };

    placesService.getDetails(request, (result, status) => {
        // eslint-disable-next-line no-undef
        if (status !== google.maps.places.PlacesServiceStatus.OK) {
            console.error('Could not get details: status is ' + status);
            return;
        }

        setLocation(result.geometry.location);
    });
}

form.addEventListener('submit', e => {
    const place = autocomplete.getPlace(); // eslint-disable-line no-undef

    if (place === undefined) {
        const request = {
            componentRestrictions: restrictions,
            input: inputName.value,
            sessionToken: sessionToken
        };

        acService.getPlacePredictions(request, getTopLocation);
    } else {
        setLocation(place.geometry.location);
    }

    e.preventDefault();
});
