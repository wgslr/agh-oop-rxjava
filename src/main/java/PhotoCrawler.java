import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import model.Photo;
import model.PhotoSize;
import util.PhotoDownloader;
import util.PhotoProcessor;
import util.PhotoSerializer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PhotoCrawler {
    private static final Logger log = Logger.getLogger(PhotoCrawler.class.getName());

    private final PhotoDownloader photoDownloader;

    private final PhotoSerializer photoSerializer;

    private final PhotoProcessor photoProcessor;

    public PhotoCrawler() throws IOException {
        this.photoDownloader = new PhotoDownloader();
        this.photoSerializer = new PhotoSerializer("./photos");
        this.photoProcessor = new PhotoProcessor();
    }

    public void resetLibrary() throws IOException {
        photoSerializer.deleteLibraryContents();
    }

    public void downloadPhotoExamples() {
        photoDownloader.getPhotoExamples()
//                .compose(this::processPhotos)
                .compose(this::processPhotosWithGroupby)
                .subscribe(photoSerializer::savePhoto);
        log.log(Level.INFO, "Finished downloads");
    }

    public void downloadPhotosForQuery(String query) throws IOException {
        photoDownloader.searchForPhotos(query)
//                .compose(this::processPhotos)
                .compose(this::processPhotosWithGroupby)
                .subscribe(photoSerializer::savePhoto);
    }

    public void downloadPhotosForMultipleQueries(List<String> queries) {
        photoDownloader.searchForPhotos(queries)
//                .compose(this::processPhotos)
                .compose(this::processPhotosWithGroupby)
                .subscribe(photoSerializer::savePhoto);
    }

    private Observable<Photo> processPhotos(Observable<Photo> source) {
        return source.filter(photoProcessor::isPhotoValid)
                .map(photoProcessor::convertToMiniature);
    }


    private Observable<Photo> processPhotosWithGroupby(Observable<Photo> source) {
        return source.filter(photoProcessor::isPhotoValid)
                .groupBy(PhotoSize::resolve)
                .flatMap(groupedObservable -> {
                            if (groupedObservable.getKey() == PhotoSize.MEDIUM) {
                                return groupedObservable.buffer(5, 0, TimeUnit.SECONDS)
                                        .flatMap(Observable::fromIterable);
                            } else {
                                return groupedObservable
                                        .observeOn(Schedulers.computation())
                                        .map(photoProcessor::convertToMiniature);
                            }
                        }
                );
    }
}
