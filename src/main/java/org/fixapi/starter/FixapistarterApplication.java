package org.fixapi.starter;

import java.io.FileInputStream;
import java.io.InputStream;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import quickfix.DefaultMessageFactory;
import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.LogFactory;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;

@SpringBootApplication
public class FixapistarterApplication {
	private static final Logger logger = LoggerFactory.getLogger(FixapistarterApplication.class);

	private static boolean devEnv = true;
	private static boolean resetSeqData = true;
	private static boolean resetSeqOrder = true;
	private static String dataCfgFileName = "data.cfg";
	private static String orderCfgFileName = "order.cfg";

	private static SocketInitiator dataInitiator = null;
	private static Data dataFIX = null;

	private static SocketInitiator orderInitiator = null;
	private static Order orderFIX = null;

	public static void main(String[] args) {
		SpringApplication.run(FixapistarterApplication.class, args);

		logger.info("Opening......");

		startDataFix();
		startOrderFix();
	}

	@PreDestroy
	public void onExit() {
		logger.info("Closing......");

		try {
			endDataFix();
			endOrderFix();
			Thread.sleep(5 * 1000);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
		}
	}

	 public static boolean startDataFix() {
		if (!devEnv) {
			if (startDataFixProd(resetSeqData)) {
				return true;
			} else {
				logger.error("Failed to start data service.");
				return false;
			}
		} else {
			if (startDataFixDev(resetSeqData)) {
				return true;
			} else {
				logger.error("Failed to start data service.");
				return false;
			}
		}
	}

	public static boolean endDataFix() {
		try {
			if (dataInitiator != null) {
				dataInitiator.stop(true);
				dataInitiator = null;
				dataFIX = null;
				return true;
			}
			return false;
		} catch (Exception e) {
			dataInitiator = null;
			dataFIX = null;
			e.printStackTrace();
			logger.error(e.getMessage());
			return false;
		}
	}

	private static boolean startDataFixProd(boolean resetReq) {
		if (dataInitiator != null) {
			dataInitiator.stop(true);
			try {
				dataInitiator.start();
				return true;
			} catch (Exception e) {
				dataInitiator = null;
				e.printStackTrace();
				logger.error(e.getMessage());
				return false;
			}
		}

		boolean result = false;

		FileInputStream fileInputStream = null;

		try {
			fileInputStream = new FileInputStream(dataCfgFileName);
			SessionSettings settings = new SessionSettings(fileInputStream);
			fileInputStream.close();
			fileInputStream = null;

			dataFIX = new Data(settings, resetReq);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings);
			LogFactory logFactory = null;
			logFactory = new FileLogFactory(settings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			dataInitiator = new SocketInitiator(dataFIX, storeFactory, settings, logFactory,
					messageFactory);

			dataInitiator.start();

			result = true;
		} catch (Exception e) {
			dataInitiator = null;
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		}

		return result;
	}

	private static boolean startDataFixDev(boolean resetReq) {
		if (dataInitiator != null) {
			dataInitiator.stop(true);
			try {
				dataInitiator.start();
				return true;
			} catch (Exception e) {
				dataInitiator = null;
				e.printStackTrace();
				logger.error(e.getMessage());
				return false;
			}
		}

		boolean result = false;

		InputStream fileInputStream = null;

		try {
			fileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(dataCfgFileName);
			SessionSettings settings = new SessionSettings(fileInputStream);
			fileInputStream.close();
			fileInputStream = null;

			dataFIX = new Data(settings, resetReq);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings);
			LogFactory logFactory = null;
			logFactory = new FileLogFactory(settings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			dataInitiator = new SocketInitiator(dataFIX, storeFactory, settings, logFactory,
					messageFactory);

			dataInitiator.start();

			result = true;
		} catch (Exception e) {
			dataInitiator = null;
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		}

		return result;
	}

	public static boolean startOrderFix() {
		if (!devEnv) {
			if (startOrderFixProd(resetSeqOrder)) {
				return true;
			} else {
				logger.error("Failed to start order service.");
				return false;
			}
		} else {
			if (startOrderFixDev(resetSeqOrder)) {
				return true;
			} else {
				logger.error("Failed to start order service.");
				return false;
			}
		}
	}

	private static boolean endOrderFix() {
		try {
			if (orderInitiator != null) {
				orderInitiator.stop(true);
				orderInitiator = null;
				orderFIX = null;
				return true;
			}
			return false;
		} catch (Exception e) {
			orderInitiator = null;
			orderFIX = null;
			e.printStackTrace();
			logger.error(e.getMessage());
			return false;
		}
	}

	private static boolean startOrderFixProd(boolean resetReq) {
		if (orderInitiator != null) {
			orderInitiator.stop(true);
			try {
				orderInitiator.start();
				return true;
			} catch (Exception e) {
				orderInitiator = null;
				e.printStackTrace();
				logger.error(e.getMessage());
				return false;
			}
		}

		boolean result = false;

		FileInputStream fileInputStream = null;

		try {
			fileInputStream = new FileInputStream(orderCfgFileName);
			SessionSettings settings = new SessionSettings(fileInputStream);
			fileInputStream.close();
			fileInputStream = null;

			orderFIX = new Order(settings, resetReq);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings);
			LogFactory logFactory = new FileLogFactory(settings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			orderInitiator = new SocketInitiator(orderFIX, storeFactory, settings, logFactory,
					messageFactory);

			orderInitiator.start();

			result = true;
		} catch (Exception e) {
			orderInitiator = null;
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		}

		return result;
	}

	private static boolean startOrderFixDev(boolean resetReq) {
		if (orderInitiator != null) {
			orderInitiator.stop(true);
			try {
				orderInitiator.start();
				return true;
			} catch (Exception e) {
				orderInitiator = null;
				e.printStackTrace();
				logger.error(e.getMessage());
				return false;
			}
		}

		boolean result = false;

		InputStream fileInputStream = null;

		try {
			fileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(orderCfgFileName);
			SessionSettings settings = new SessionSettings(fileInputStream);
			fileInputStream.close();
			fileInputStream = null;

			orderFIX = new Order(settings, resetReq);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings);
			LogFactory logFactory = new FileLogFactory(settings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			orderInitiator = new SocketInitiator(orderFIX, storeFactory, settings, logFactory,
					messageFactory);

			orderInitiator.start();

			result = true;
		} catch (Exception e) {
			orderInitiator = null;
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
			}
		}

		return result;
	}
}
