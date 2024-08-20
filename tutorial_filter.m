% this is a new script from a youtube video on filtering EEG signals
% here ia new line

% Load the EEGlab set file
EEG = pop_loadset('your_file.set');

% Define the notch filter parameters
notch_freq = 60; % frequency to notch (Hz)
notch_width = 2; % width of the notch (Hz)
fs = EEG.srate; % sampling frequency (Hz)

% Create a notch filter
[b, a] = iirnotch(notch_freq, notch_width, fs);

% Filter the EEG data
EEG.data = filter(b, a, EEG.data);

% Save the filtered EEG data
EEG = pop_saveset(EEG, 'your_filtered_file.set');