% Load the EEGlab set file
mypath = "tutorialcode";
EEG = pop_loadset('your_file.set');

% Define the spectrogram parameters
window_size = 256; % window size (samples)
overlap = 0.5; % overlap between windows (0-1)
nfft = 512; % FFT size (samples)
fs = EEG.srate; % sampling frequency (Hz)

% Compute the spectrogram
[S, f, t] = spectrogram(EEG.data, window_size, round(window_size*overlap), nfft, fs);

% Plot the spectrogram
figure;
imagesc(t, f, 10*log10(abs(S)));
xlabel('Time (s)');
ylabel('Frequency (Hz)');
title('Spectrogram of EEG Data');
colormap(jet);
colorbar;

% Add a notch filter line at 60 Hz
hold on;
plot(t, ones(size(t))*60, 'k--');