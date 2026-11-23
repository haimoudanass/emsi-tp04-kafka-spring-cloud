package ma.emsi.tp04.kafka.api;

import ma.emsi.tp04.kafka.domain.VisitEvent;
import ma.emsi.tp04.kafka.messaging.VisitPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = VisitPublishController.class)
class VisitPublishControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private VisitPublisher visitPublisher;

    @Test
    void publishAccepted() throws Exception {
        when(visitPublisher.publish(eq("visit-ingress"), any(VisitEvent.class))).thenReturn(true);

        mockMvc.perform(post("/api/visits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"pageId":"catalog","visitorId":"v9","dwellMs":300,"topic":"visit-ingress"}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.pageId").value("catalog"))
                .andExpect(jsonPath("$.visitorId").value("v9"));
    }

    @Test
    void publishValidationError() throws Exception {
        mockMvc.perform(post("/api/visits")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"pageId":"","topic":"visit-ingress"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void demoEndpoint() throws Exception {
        when(visitPublisher.publish(eq("visit-ingress"), any(VisitEvent.class))).thenReturn(true);

        mockMvc.perform(post("/api/visits/demo"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.topic").value("visit-ingress"));
    }
}
