// requires jQuery
function showError(error) {
    $("#error-overlay").show();
    $("#error-overlay .toast-body").text(error);
    $("#error-overlay .toast").show();

    $("#error-overlay .close").one("click", function () {
        $("#error-overlay .toast").hide();
        $("#error-overlay").hide();
    });
}

// requires d3
function hydratePalette(serPalette) {
    let dateParse = d3.timeParse("%Y-%m-%d"),
        palette = {};

    for (const [key, serPerson] of Object.entries(serPalette)) {
        let person = {}

        if (serPerson.timed) {
            let serTimed = serPerson.timed, timed = [];

            for (let j = 0; j < serTimed.length; ++j) {
                let serStop = serTimed[j], stop = [ dateParse(serStop[0]) ];
                if (serStop.length == 3) {
                    stop.push(dateParse(serStop[1]));
                    stop.push(serStop[2]);
                }

                timed.push(stop);
            }

            person.timed = timed;
        }

        if (serPerson.started) {
            person.started = [ dateParse(serPerson.started[0]), serPerson.started[1] ];
        }

        if (serPerson.ended) {
            person.ended = [ dateParse(serPerson.ended[0]), serPerson.ended[1] ];
        }

        if (serPerson['default']) {
            person['default'] = serPerson['default'];
        }

        palette[key] = person;
    }

    return palette;
}

// ported from https://en.cppreference.com/w/cpp/algorithm/lower_bound
function lowerBound(array, value) {
    let first = 0, count = array.length;

    while (count > 0) {
        let it = first, step = count >>> 1;
        it += step;
        if (array[it] < value) {
            first = ++it;
            count -= (step + 1);
        } else {
            count = step;
        }
    }

    return first;
}

// requires loaded palette
function getColor(person_id, day) {
    if (!person_id) {
        return "#FFF";
    }

    let person = palette[person_id.toString()];
    if (!person) {
        return "#FFF";
    }

    if (person.timed) {
        let timed = person.timed, i = lowerBound(timed, [ day ]);
        if (i < timed.length) {
            let stop = timed[i];

            // the search above actually doesn't search for from
            // stops, but for something a bit smaller...
            if ((i + 1) < timed.length) {
                let nextStop = timed[i + 1];
                if (nextStop[0] == day) {
                    return nextStop[2];
                }
            }

            if (stop[0] == day) {
                if (stop.length == 1) {
                    stop = timed[i - 1];
                }

                return stop[2];
            } else {
                if ((stop.length == 3) && (stop[0] < stop[1])) {
                    return stop[2];
                }
            }
        }
    }

    if (person.started) {
        let started = person.started;
        if (day >= started[0]) {
            return started[1];
        }
    }

    if (person.ended) {
        let ended = person.ended;
        if (day <= ended[0]) {
            return ended[1];
        }
    }

    return person['default'] || "#FFF";
}
